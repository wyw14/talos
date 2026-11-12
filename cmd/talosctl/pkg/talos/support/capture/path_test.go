// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package capture_test

import (
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/siderolabs/talos/cmd/talosctl/pkg/talos/support/capture"
)

func TestValidateArchivePath(t *testing.T) {
	t.Parallel()

	valid := []string{
		"dmesg.log",
		"n1/dmesg.log",
		"kubernetes-logs/kube-system/pod.log",
		"n1/service-logs/./etcd.log",
		"resources/MachineStatus.talos.dev.yaml",
	}

	for _, p := range valid {
		require.NoError(t, capture.ValidateArchivePath(p), "expected %q to be accepted", p)
	}

	invalid := []string{
		"",
		"../escape",
		"a/../../b",
		"..",
		"/etc/passwd",
		`\windows`,
		`C:\windows`,
		"C:relative",
		"nul\x00byte",
		"line\nbreak",
		"a/../b",
	}

	for _, p := range invalid {
		err := capture.ValidateArchivePath(p)
		require.Error(t, err, "expected %q to be rejected", p)
		assert.NotEmpty(t, err.Error())
	}
}

func TestPathTrackerRejectsDuplicates(t *testing.T) {
	t.Parallel()

	tracker := capture.NewPathTracker()

	require.NoError(t, tracker.Register("n1/dmesg.log"))

	err := tracker.Register("n1//dmesg.log")
	require.Error(t, err)
	assert.ErrorIs(t, err, capture.ErrPathDuplicate)

	// backslash spelling of the same cleaned path collides too
	err = tracker.Register(`n1\dmesg.log`)
	require.Error(t, err)
	assert.ErrorIs(t, err, capture.ErrPathDuplicate)

	require.NoError(t, tracker.Register("n2/dmesg.log"))
}

func TestPathTrackerRejectsUnsafe(t *testing.T) {
	t.Parallel()

	tracker := capture.NewPathTracker()

	assert.Error(t, tracker.Register("../x"))
	assert.Error(t, tracker.Register(""))
}

func TestResolveWorkspacePath(t *testing.T) {
	t.Parallel()

	root := filepath.Clean(t.TempDir())

	inside, err := capture.ResolveWorkspacePath(root, filepath.Join("chunks", "1.chunk"))
	require.NoError(t, err)
	assert.True(t, strings.HasPrefix(inside, root))

	_, err = capture.ResolveWorkspacePath(root, filepath.Join("..", "outside"))
	require.Error(t, err)

	_, err = capture.ResolveWorkspacePath(root, root)
	require.Error(t, err)
}
