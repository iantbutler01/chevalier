//go:build windows

package mount

import (
	"unsafe"

	"github.com/winfsp/go-winfsp"
	"github.com/winfsp/go-winfsp/gofs"
	"golang.org/x/sys/windows"
)

type fileBasicInfo struct {
	creationTime   int64
	lastAccessTime int64
	lastWriteTime  int64
	changeTime     int64
	attributes     uint32
	_              uint32
}

func (f *TrackedFile) SetBasicInfo(
	flags winfsp.SetBasicInfoFlags,
	attributes uint32,
	creationTime, lastAccessTime, lastWriteTime, changeTime uint64,
) error {
	path, err := windows.UTF16PtrFromString(f.File.Name())
	if err != nil {
		return err
	}
	handle, err := windows.CreateFile(
		path,
		windows.FILE_READ_ATTRIBUTES|windows.FILE_WRITE_ATTRIBUTES,
		windows.FILE_SHARE_READ|windows.FILE_SHARE_WRITE|windows.FILE_SHARE_DELETE,
		nil,
		windows.OPEN_EXISTING,
		windows.FILE_FLAG_BACKUP_SEMANTICS,
		0,
	)
	if err != nil {
		return err
	}
	defer windows.CloseHandle(handle)

	var info fileBasicInfo
	if flags&winfsp.SetBasicInfoAttributes != 0 {
		attributes &^= windows.FILE_ATTRIBUTE_DIRECTORY | windows.FILE_ATTRIBUTE_REPARSE_POINT
		if attributes == 0 {
			attributes = windows.FILE_ATTRIBUTE_NORMAL
		}
		info.attributes = attributes
	}
	if flags&winfsp.SetBasicInfoCreationTime != 0 {
		info.creationTime = int64(creationTime)
	}
	if flags&winfsp.SetBasicInfoLastAccessTime != 0 {
		info.lastAccessTime = int64(lastAccessTime)
	}
	if flags&winfsp.SetBasicInfoLastWriteTime != 0 {
		info.lastWriteTime = int64(lastWriteTime)
	}
	if flags&winfsp.SetBasicInfoChangeTime != 0 {
		info.changeTime = int64(changeTime)
	}
	return windows.SetFileInformationByHandle(
		handle,
		windows.FileBasicInfo,
		(*byte)(unsafe.Pointer(&info)),
		uint32(unsafe.Sizeof(info)),
	)
}

var _ gofs.FileSetBasicInfo = (*TrackedFile)(nil)
