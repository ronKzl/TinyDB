//go:build unix

package kvdb

import (
	"os"
	"path"
	"syscall"
)

// createFileSync opens or creates a file and synchronizes the parent directory's 
// metadata to ensure the file's existence is durable on disk.
func createFileSync(file string) (*os.File, error) {
	fp, err := os.OpenFile(file, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return nil, err
	}
	if err = syncDir(path.Base(file)); err != nil {
		_ = fp.Close()
		return nil, err
	}
	return fp, err
}

// syncDir performs an fsync on the parent directory of the specified file. 
// This is required on Unix systems to guarantee that metadata changes, 
// such as file creation or deletion, are committed to stable storage.
func syncDir(file string) error {
	flags := os.O_RDONLY | syscall.O_DIRECTORY
	dirfd, err := syscall.Open(path.Dir(file), flags, 0o644)
	if err != nil {
		return err
	}
	defer syscall.Close(dirfd)
	// Flush the directory's file descriptor to disk.
	return syscall.Fsync(dirfd)
}

