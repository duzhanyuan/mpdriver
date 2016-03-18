package server

import (
	"encoding/json"
	"io"

	"github.com/ngaut/log"
)

// CmdRecord represnets a command record.
type CmdRecord struct {
	ConnID uint64
	Query  string
	Args   []interface{}
}

var newLine []byte = []byte("\r\n")

func WriteCmdRecord(connID uint64, query string, args ...interface{}) {
	if CommandRecordWriter != nil {
		cmd := &CmdRecord{
			ConnID: connID,
			Query:  query,
			Args:   args,
		}
		b, err := json.Marshal(cmd)
		if err != nil {
			log.Errorf("failed to marshal json %v", err)
		}
		_, err = CommandRecordWriter.Write(b)
		if err != nil {
			log.Errorf("failed to write command record %v", err)
		}
		CommandRecordWriter.Write(newLine)
	}
}

var CommandRecordWriter io.Writer
