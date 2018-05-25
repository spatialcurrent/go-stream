// =================================================================
//
// Copyright (C) 2018 Spatial Current, Inc. - All Rights Reserved
// Released as open source under the MIT License.  See LICENSE file.
//
// =================================================================

package stream

import (
	"bufio"
	"bytes"
	"fmt"
	"compress/gzip"
)

import (
	"github.com/golang/snappy"
	"github.com/pkg/errors"
)

type MemoryBlock struct {
  Algorithm string         `xml:"-" json:"-"`
  BigEndian bool `xml:"-" json:"-"`
  Bytes     []byte         `xml:"-" json:"-"`
}

func (mb *MemoryBlock) Size() (int64, error) {
  return int64(len(mb.Bytes)), nil
}

func (mb *MemoryBlock) Reader() (*Reader, error) {
  switch mb.Algorithm {
  case "snappy":
		return &Reader{Reader: snappy.NewReader(bytes.NewReader(mb.Bytes))}, nil
  case "gzip":
		gr, err := gzip.NewReader(bytes.NewReader(mb.Bytes))
		if gr != nil {
			return nil, errors.Wrap(err, "Error creating gzip reader for memory block.")
		}
		return &Reader{ReadCloser: gr}, nil
  case "none":
		return &Reader{Reader: bufio.NewReader(bytes.NewReader(mb.Bytes))}, nil
  }
  return nil, errors.New("Unknown compression algorithm")
}

func (mb *MemoryBlock) Iterator() (*BlockIterator, error) {
  reader, err := mb.Reader()
  if err != nil {
    return &BlockIterator{}, errors.Wrap(err, "Error creating iterator")
  }

  it := &BlockIterator{
    Reader: reader,
    BigEndian: mb.BigEndian,
  }

  return it, nil
}

func (mb *MemoryBlock) Get(position int) ([]byte, error) {
	it, err := mb.Iterator()
	if err != nil {
		return make([]byte,0), errors.Wrap(err, "Error creating iterator to get bytes at position "+fmt.Sprint(position)+" in block")
	}

	err = it.Skip(position)
	if err != nil {
		return make([]byte,0), errors.Wrap(err, "Error skipping to position "+fmt.Sprint(position)+" in block.")
	}

	return it.Next()
}

func (mb *MemoryBlock) Init(b []byte) error {
  mb.Bytes = b
	return nil
}

func (mb *MemoryBlock) Remove() error {
	mb.Bytes = make([]byte, 0)
	return nil
}

func NewMemoryBlock(algorithm string, bigEndian bool) *MemoryBlock {
  return &MemoryBlock{Algorithm: algorithm, BigEndian: bigEndian}
}
