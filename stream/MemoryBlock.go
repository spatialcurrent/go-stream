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

// MemoryBlock holds a block of objects in memory.
// The objects may be compressed using the Algorithm.
type MemoryBlock struct {
	AbstractBlock
  Bytes     []byte         `xml:"-" json:"-"` // The compressed block of bytes
}

// Size returns the number of bytes as an int64.
func (mb *MemoryBlock) Size() (int64, error) {
  return int64(len(mb.Bytes)), nil
}

// Reader returns a *Reader for reading the compressed bytes, and an error if any.
func (mb *MemoryBlock) Reader() (*Reader, error) {
  switch mb.GetAlgorithm() {
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

// Iterator returns a BlockIterator for iterating through the bytes, and an error if any.
func (mb *MemoryBlock) Iterator() (*BlockIterator, error) {
  reader, err := mb.Reader()
  if err != nil {
    return &BlockIterator{}, errors.Wrap(err, "Error creating iterator")
  }

  it := &BlockIterator{
    Reader: reader,
    BigEndian: mb.UseBigEndian(),
  }

  return it, nil
}

// Get returns the bytes for an object at an arbitrary position, and an error if any.
// Given the compressed nature of the data, Get starts reading from the beginning every time.
// If you're iterating through the data, use Iterator.  Only use this function for random access.
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

// Init initializes a block's data.
func (mb *MemoryBlock) Init(b []byte) error {
  mb.Bytes = b
	return nil
}

// Remove clears the Bytes array.
func (mb *MemoryBlock) Remove() error {
	mb.Bytes = make([]byte, 0)
	return nil
}

// NewMemoryBlock returns a new MemoryBlock.
// Algorithm can be snappy, gzip, or none.
// If bigEndian is true, then encodes numbers using a big-endian byte order, else encodes using littl-endian byte order.
func NewMemoryBlock(algorithm string, bigEndian bool) *MemoryBlock {
  return &MemoryBlock{
		AbstractBlock: AbstractBlock{
				Algorithm: algorithm,
				BigEndian: bigEndian,
			},
		}
}
