// =================================================================
//
// Copyright (C) 2018 Spatial Current, Inc. - All Rights Reserved
// Released as open source under the MIT License.  See LICENSE file.
//
// =================================================================

package stream

import (
	"encoding/binary"
	"fmt"
	"io"
)

import (
	"github.com/pkg/errors"
)

var MAXIMUM_SLICE_LENGTH = 400

// BlockIterator is an iterator for reading the data in a block
type BlockIterator struct {
  Reader *Reader
  BigEndian bool
}

func (it *BlockIterator) Next() ([]byte, error) {

  header := make([]byte, 0, 8)
  for {
    b := make([]byte, 8-len(header))
    n, err := it.Reader.Read(b)
    header = append(header, b[:n]...)
    if err != nil {
      if err == io.EOF {
        return header, err
      } else {
        return header, errors.Wrap(err, "Error reading content size header from stream.")
      }
    }
    if len(header) == 8 {
      break
    }
  }
  size := 0
  if it.BigEndian {
    size = int(binary.BigEndian.Uint64(header))
  } else {
    size = int(binary.LittleEndian.Uint64(header))
  }
  if size > MAXIMUM_SLICE_LENGTH {
    return []byte{}, errors.New("Size " + fmt.Sprint(size) + " exceeds maximum slice length of "+fmt.Sprint(MAXIMUM_SLICE_LENGTH)+".")
  }
  content := make([]byte, 0, size)
  for {
    b := make([]byte, size-len(content))
    n, err := it.Reader.Read(b)
    content = append(content, b[:n]...)
    if err != nil {
      if err == io.EOF {
        return content, err
      } else {
        return content, errors.Wrap(err, "Error reading content from byte stream.")
      }
    }
    if len(content) == size {
      break
    }
  }

  return content, nil
}

func (it *BlockIterator) Skip(n int) error {
	for i := 0; i < n; i++ {
		_, err := it.Next()
		if err != nil {
			return errors.Wrap(err, "Error skipping while at count "+fmt.Sprint(i))
		}
	}
	return nil
}

func (it *BlockIterator) Close() error {
	return it.Reader.Close()
}
