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

type Reader struct {
  Reader io.Reader
  ReadCloser io.ReadCloser
  File *os.File
}

func (r *Reader ) Read(p []byte) (n int, err error) {

  if r.ReadCloser != nil {
    return r.ReadCloser.Read(p)
  }

  if r.Reader != nil {
    return r.Reader.Read(p)
  }

  return 0, nil
}

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
