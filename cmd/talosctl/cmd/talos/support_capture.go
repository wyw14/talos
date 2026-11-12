// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package talos

import (
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/siderolabs/go-talos-support/support/bundle"
	"github.com/siderolabs/go-talos-support/support/bundle/encryption"
	"github.com/siderolabs/go-talos-support/support/collectors"
	k8s "k8s.io/client-go/kubernetes"

	"github.com/siderolabs/talos/cmd/talosctl/pkg/talos/global"
	"github.com/siderolabs/talos/cmd/talosctl/pkg/talos/safeout"
	"github.com/siderolabs/talos/cmd/talosctl/pkg/talos/support/capture"
	"github.com/siderolabs/talos/pkg/machinery/api/machine"
)

// runLegacySupport keeps the pre-capture behavior: collectors stream their results straight into
// the (optionally encrypted) zip archive, without a workspace or a checkpoint.
func runLegacySupport(ctx context.Context, outputPath string, progress chan bundle.Progress,
	clientFactory *global.ClientFactory, encryptionOpts []encryption.Option,
) error {
	f, err := openOutputFile(outputPath)
	if err != nil {
		return err
	}

	defer f.Close() //nolint:errcheck

	var (
		archiveOutput io.Writer = f
		encWriter     io.WriteCloser
	)

	if !supportCmdFlags.noEncryption {
		encWriter, err = encryption.Encrypt(f, encryptionOpts...)
		if err != nil {
			return err
		}

		archiveOutput = encWriter
	}

	collectErr := collectData(ctx, archiveOutput, progress, clientFactory)

	// flush the age encryption layer before reporting success
	if encWriter != nil {
		if err = encWriter.Close(); err != nil {
			return err
		}
	}

	return collectErr
}

// runCapture runs the resumable capture-artifact mode: collectors write sealed chunks into a
// workspace, and the final archive is assembled from them in a single fresh pass.
func runCapture(ctx context.Context, outputPath string, progress chan bundle.Progress,
	clientFactory *global.ClientFactory, encryptionOpts []encryption.Option,
) error {
	nodeCtx, c, err := clientFactory.BuildClientFirstNode(ctx)
	if err != nil {
		return err
	}

	var clientset *k8s.Clientset

	// without Kubernetes configured, the Kubernetes collectors are skipped
	if kubernetesConfigured(nodeCtx, c) {
		clientset, err = getKubernetesClient(nodeCtx, c)
		if err != nil {
			fmt.Fprintf(safeout.Stderr(), "warning: failed to create kubernetes client: %s\n", err)
		}
	}

	bundleOpts := []bundle.Option{
		bundle.WithKubernetesClient(clientset),
		bundle.WithTalosClientProvider(clientFactory.BuildClient),
		bundle.WithNodes(clientFactory.Nodes()...),
		bundle.WithNumWorkers(supportCmdFlags.numWorkers),
		bundle.WithProgressChan(progress),
	}

	if !supportCmdFlags.verbose {
		bundleOpts = append(bundleOpts, bundle.WithLogOutput(io.Discard))
	}

	options := bundle.NewOptions(bundleOpts...)

	cols, err := collectors.GetForOptions(ctx, options)
	if err != nil {
		return fmt.Errorf("failed to create collectors: %w", err)
	}

	redaction, probeErr := probeRedaction(ctx, clientFactory)
	if probeErr != nil {
		fmt.Fprintf(safeout.Stderr(), "warning: redaction protocol detection failed for some nodes: %s\n", probeErr)
	}

	recipientKeys, err := resolvedRecipientLines()
	if err != nil {
		return err
	}

	encryptionConfig := capture.NewEncryptionConfig(!supportCmdFlags.noEncryption, recipientKeys)
	runConfig := capture.BuildRunConfig(
		cols,
		clientFactory.Nodes(),
		supportCmdFlags.CertFingerprints,
		supportCmdFlags.Insecure,
		redaction,
		encryptionConfig,
	)

	workspaceDir := supportCmdFlags.captureDir
	if workspaceDir == "" {
		workspaceDir = outputPath + ".capture"
	}

	captureOpts := []capture.Option{
		capture.WithWorkspaceDir(workspaceDir),
		capture.WithTTL(supportCmdFlags.captureTTL),
		capture.WithMaxCollectorBytes(int64(supportCmdFlags.captureMaxCollector.Bytes())),
		capture.WithMaxDiskBytes(int64(supportCmdFlags.captureMaxDisk.Bytes())),
		capture.WithNumWorkers(supportCmdFlags.numWorkers),
		capture.WithProgressChan(progress),
	}

	if supportCmdFlags.verbose {
		captureOpts = append(captureOpts, capture.WithLogOutput(safeout.Stderr()))
	} else {
		captureOpts = append(captureOpts, capture.WithLogOutput(io.Discard))
	}

	engine, err := capture.NewEngine(runConfig, captureOpts...)
	if err != nil {
		return err
	}

	if _, err = engine.Run(ctx, cols); err != nil {
		return err
	}

	var sink capture.EncryptionSink = capture.PlainSink{}

	if !supportCmdFlags.noEncryption {
		sink = capture.AgeSink{Options: encryptionOpts}
	}

	assembly, err := engine.Assemble(outputPath, sink)
	if err != nil {
		return err
	}

	if assembly.Partial {
		fmt.Fprintf(safeout.Stderr(),
			"support bundle is partial: failed collectors are recorded in %s; rerun the same command to resume, sealed artifacts in %s are reused\n",
			capture.ManifestName, workspaceDir)
	}

	return nil
}

// probeRedaction asks every node for its redaction feature bit. A probe failure is visible to the
// operator but non-fatal: the node is recorded as not supporting redaction, matching the fallback
// behavior of the collector package.
func probeRedaction(ctx context.Context, clientFactory *global.ClientFactory) (map[string]bool, error) {
	result := make(map[string]bool, len(clientFactory.Nodes()))

	var errs error

	for _, node := range clientFactory.Nodes() {
		nodeCtx, nodeClient, err := clientFactory.BuildClient(ctx, node)
		if err != nil {
			errs = errors.Join(errs, fmt.Errorf("node %s: %w", node, err))
			result[node] = false

			continue
		}

		response, err := nodeClient.Version(nodeCtx)
		if err != nil {
			errs = errors.Join(errs, fmt.Errorf("node %s: %w", node, err))
			result[node] = false

			continue
		}

		result[node] = redactionSupported(response)
	}

	return result, errs
}

// redactionSupported mirrors the feature detection used by the collector package: every version
// message must report the resource redaction feature.
func redactionSupported(response *machine.VersionResponse) bool {
	messages := response.GetMessages()
	if len(messages) == 0 {
		return false
	}

	for _, msg := range messages {
		if !msg.GetFeatures().GetResourceRedaction() {
			return false
		}
	}

	return true
}

// resolvedRecipientLines returns the full set of public recipient lines the bundle is encrypted
// to, including the embedded defaults. Only public key material is handled here.
func resolvedRecipientLines() ([]string, error) {
	if supportCmdFlags.noEncryption {
		return nil, nil
	}

	if supportCmdFlags.encryptionNoDefaultRecipients {
		return append([]string(nil), supportCmdFlags.encryptionRecipients...), nil
	}

	defaults, err := encryption.DefaultRecipients()
	if err != nil {
		return nil, err
	}

	var lines []string

	for _, recipient := range defaults {
		lines = append(lines, recipient.PublicKeys...)
	}

	return append(lines, supportCmdFlags.encryptionRecipients...), nil
}
