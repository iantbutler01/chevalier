//go:build !windows

package mount

func normalizeLookupError(_ string, err error) error {
	return err
}
