// =================================================================
//
// Copyright (C) 2018 Spatial Current, Inc. - All Rights Reserved
// Released as open source under the MIT License.  See LICENSE file.
//
// =================================================================

package stream

// Block is an interface for a compressed array of objects in a binary representation
type Block interface {
  Init(b []byte) error // initialize block
  Size() (int64, error) // get size of block in bytes
  Reader() (*Reader, error) // get reader for this block
  Iterator() (*BlockIterator, error) // get iterator for this block
  Get(position int) ([]byte, error) // get object at the given position
  Remove() error // remove block
}
