package mount

import (
	"testing"
	"time"

	"github.com/winfsp/go-winfsp/treelock"
)

func TestDeletePathLockWaitsForMetadataReader(t *testing.T) {
	locker := treelock.New()
	node := locker.AllocSlash("/workspace/file.txt")
	defer node.Free()
	reader := node.RLockPath()

	acquired := make(chan *treelock.PathLock, 1)
	go func() {
		acquired <- node.WLockPath()
	}()

	select {
	case lock := <-acquired:
		lock.Unlock()
		t.Fatal("delete lock bypassed an active metadata reader")
	case <-time.After(25 * time.Millisecond):
	}

	reader.Unlock()
	select {
	case lock := <-acquired:
		if lock == nil {
			t.Fatal("delete lock unexpectedly failed")
		}
		lock.Unlock()
	case <-time.After(time.Second):
		t.Fatal("delete lock did not wake after metadata reader exited")
	}
}

func TestDeleteOpenLockWaitsForMetadataReader(t *testing.T) {
	locker := treelock.New()
	reader := locker.RLockSlash("/workspace/file.txt")

	acquired := make(chan *treelock.PathLock, 1)
	go func() {
		acquired <- locker.WLockSlash("/workspace/file.txt")
	}()

	select {
	case lock := <-acquired:
		lock.Unlock()
		t.Fatal("delete-open lock bypassed an active metadata reader")
	case <-time.After(25 * time.Millisecond):
	}

	reader.Unlock()
	select {
	case lock := <-acquired:
		if lock == nil {
			t.Fatal("delete-open lock unexpectedly failed")
		}
		lock.Unlock()
	case <-time.After(time.Second):
		t.Fatal("delete-open lock did not wake after metadata reader exited")
	}
}
