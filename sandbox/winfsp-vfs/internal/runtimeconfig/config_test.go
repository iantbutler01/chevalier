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

func TestHasDesktopSessionMatchesQueryUserRowsCaseInsensitively(t *testing.T) {
	output := " USERNAME              SESSIONNAME        ID  STATE   IDLE TIME  LOGON TIME\r\n openbracket           console             1  Active      none   8/30/2026 5:36 PM\r\n"
	if !hasDesktopSession(output, "OpenBracket") {
		t.Fatal("expected OpenBracket console session")
	}
	if hasDesktopSession(output, "OtherUser") {
		t.Fatal("unexpected session match")
	}
}
