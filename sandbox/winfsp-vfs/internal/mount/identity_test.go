package mount

import (
	"path/filepath"
	"testing"
)

func TestMountIdentityRejectsScopeOrVMReuse(t *testing.T) {
	root := filepath.Join(t.TempDir(), "state")
	identity := "vm-1\nhttps://gateway/owner\nscope-1"
	if err := EnsureIdentity(root, identity); err != nil {
		t.Fatal(err)
	}
	if err := EnsureIdentity(root, identity); err != nil {
		t.Fatal(err)
	}
	if err := EnsureIdentity(root, "vm-1\nhttps://gateway/owner\nscope-2"); err == nil {
		t.Fatal("rebound a state tree to a different scope")
	}
}

func TestStateLockIsExclusive(t *testing.T) {
	root := t.TempDir()
	first, err := acquireStateLock(filepath.Join(root, "owner.lock"))
	if err != nil {
		t.Fatal(err)
	}
	defer first.Close()
	if second, err := acquireStateLock(filepath.Join(root, "owner.lock")); err == nil {
		second.Close()
		t.Fatal("opened the same VFS state concurrently")
	}
}
