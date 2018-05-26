// =================================================================
//
// Copyright (C) 2018 Spatial Current, Inc. - All Rights Reserved
// Released as open source under the MIT License.  See LICENSE file.
//
// =================================================================

package stream

// Iterator is an interface implemented by StreamIterator and BlockIterator used for iterating through objects.
type Iterator interface {
  Next() ([]byte, error) // returns the current object and advances forward.
  Close() error // close the underlying Reader.
}
