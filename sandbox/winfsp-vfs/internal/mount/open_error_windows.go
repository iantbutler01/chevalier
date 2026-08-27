//go:build windows

package mount

import (
	"errors"
	"os"
	"path/filepath"
	"syscall"
)

func normalizeLookupError(local string, err error) error {
	_, parentErr := os.Stat(filepath.Dir(local))
	if parentErr != nil && (errors.Is(err, syscall.ENOTDIR) ||
		errors.Is(parentErr, os.ErrNotExist) ||
		errors.Is(parentErr, syscall.ERROR_FILE_NOT_FOUND) ||
		errors.Is(parentErr, syscall.ERROR_PATH_NOT_FOUND)) {
		return &os.PathError{Op: "open", Path: local, Err: os.ErrNotExist}
	}
	return err
}
