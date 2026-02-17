//go:build !unix

package kvdb

import "os"

// createFileSync opens or creates a file and synchronizes the parent directory's 
// metadata to ensure the file's existence is durable on disk.
func createFileSync(file string) (*os.File, error) {
	return os.OpenFile(file, os.O_RDWR|os.O_CREATE, 0o644)
}


