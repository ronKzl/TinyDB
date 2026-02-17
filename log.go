package kvdb

import (
	"io"
	"os"
)

// Log manages the persistent write-ahead log file used for crash recovery 
// and data durability.
type Log struct {
	FileName string
	fp       *os.File
}

// Open initializes the log file and prepares it for I/O operations.
func (log *Log) Open() (err error) {
	log.fp, err = createFileSync(log.FileName)
	return err
}

// Close terminates the connection to the underlying log file.
func (log *Log) Close() error {
	return log.fp.Close()
}

// Write serializes the provided Entry and appends it to the end of the log file.
func (log *Log) Write(ent *Entry) error {
	_, err := log.fp.Write(ent.Encode())
	return err
}

// Read deserializes the next Entry from the log file. It returns eof as true 
// when the end of the file is reached.
func (log *Log) Read(ent *Entry) (eof bool, err error) {
	err = ent.Decode(log.fp)
	if err == io.EOF {
		return true, nil
	} else if err != nil {
		return false, err
	} else {
		return false, nil
	}
}

