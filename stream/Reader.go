// =================================================================
//
// Copyright (C) 2018 Spatial Current, Inc. - All Rights Reserved
// Released as open source under the MIT License.  See LICENSE file.
//
// =================================================================

package stream

import (
  "io"
  "os"
)

import (
	"github.com/pkg/errors"
)

// Reader is a struct for easily closing underlying file points once a read is complete.
// Reader should only be used in situations where an *os.File is only used by a single io.Rreader.
type Reader struct {
  Reader io.Reader
  ReadCloser io.ReadCloser
  File *os.File
}

// Read reads the
func (r *Reader ) Read(p []byte) (n int, err error) {

  if r.ReadCloser != nil {
    return r.ReadCloser.Read(p)
  }

  if r.Reader != nil {
    return r.Reader.Read(p)
  }

  return 0, nil
}

// Close closes the ReadCloser, if any and the underlying os.File.
func (r *Reader) Close() error {

  if r.ReadCloser != nil {
    err := r.ReadCloser.Close()
    if err !=  nil {
      return errors.Wrap(err, "Error closing read closer.")
    }
  }

  if r.File != nil {
    err := r.File.Close()
    if err != nil {
      return errors.Wrap(err, "Error closing file.")
    }
  }

  return nil
}
