//go:build !unix

package kvdb

import "os"

func createFileSync(file string) (*os.File, error) {
	return os.OpenFile(file, os.O_RDWR|os.O_CREATE, 0o644)
}


