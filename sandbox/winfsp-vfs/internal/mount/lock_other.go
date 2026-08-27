//go:build !windows

package mount

import (
	"errors"
	"fmt"
	"os"
	"syscall"
)

type stateLock struct {
	file *os.File
}

func acquireStateLock(path string) (*stateLock, error) {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, err
	}
	if err := syscall.Flock(int(file.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		file.Close()
		return nil, fmt.Errorf("lock VFS state: %w", err)
	}
	return &stateLock{file: file}, nil
}

func (l *stateLock) Close() error {
	unlockErr := syscall.Flock(int(l.file.Fd()), syscall.LOCK_UN)
	return errors.Join(unlockErr, l.file.Close())
}
