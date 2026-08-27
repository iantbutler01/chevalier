package runtimeconfig

import "testing"

func TestConfigValidate(t *testing.T) {
	config := Config{
		SchemaVersion: SchemaVersion,
		VMID:          "vm-1",
		Generation:    "generation-1",
		Control: ControlConfig{
			ListenAddress: DefaultListen,
			TokenFile:     DefaultControlToken,
		},
		VFS: VFSConfig{
			Endpoint:       "http://10.0.2.2:63339/internal/chevalier/vfs/owner",
			Scope:          "workspace/repo",
			TokenFile:      DefaultVFSToken,
			StateDirectory: DefaultStateRoot,
			Mountpoint:     DefaultMountpoint,
			StatusFile:     DefaultStatusPath,
			DrainTimeout:   "30s",
		},
	}
	if err := config.Validate(); err != nil {
		t.Fatalf("valid config rejected: %v", err)
	}
	config.Control.ListenAddress = "not-an-address"
	if err := config.Validate(); err == nil {
		t.Fatal("invalid listen address accepted")
	}
}
