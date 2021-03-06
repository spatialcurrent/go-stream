// =================================================================
//
// Copyright (C) 2018 Spatial Current, Inc. - All Rights Reserved
// Released as open source under the MIT License.  See LICENSE file.
//
// =================================================================

package stream

import (
	"bufio"
	"compress/gzip"
	"fmt"
	"io/ioutil"
	"os"
)

import (
	"github.com/mitchellh/go-homedir"
	"github.com/golang/snappy"
	"github.com/pkg/errors"
)

// TempFileBlock is struct for reading & writing a compressed block of objects to a temporary file on disk.
type TempFileBlock struct {
	AbstractBlock
	TempDir string `xml:"-" json:"-"`
	TempDirExpanded string `xml:"-" json:"-"`
  TempFile string `xml:"-" json:"-"`
}

// Size returns the number of bytes in the block, by using os.FileInfo.Size(), and an error if any.
func (tfb *TempFileBlock) Size() (int64, error) {
	f, err := os.Open(tfb.TempFile)
	if err != nil {
		return 0, errors.Wrap(err, "Error opening file block at \""+tfb.TempFile+"\" for reading")
	}
	fi, err := f.Stat()
	if err != nil {
		return 0, errors.Wrap(err, "Error getting file info for file block at \""+tfb.TempFile+"\"")
	}
	return fi.Size(), nil
}

// Reader returns a Reader for reading the data in the block, and an error if any.
func (tfb *TempFileBlock) Reader() (*Reader, error) {
  switch tfb.GetAlgorithm() {
  case "snappy":
		f, err := os.OpenFile(tfb.TempFile, os.O_RDONLY, 0600)
		if err != nil {
			return nil, errors.Wrap(err, "Error opening file block at \""+tfb.TempFile+"\" for reading")
		}
		return &Reader{Reader: snappy.NewReader(bufio.NewReader(f)), File: f}, nil
  case "gzip":
		f, err := os.OpenFile(tfb.TempFile, os.O_RDONLY, 0600)
		if err != nil {
			return nil, errors.Wrap(err, "Error opening file block at \""+tfb.TempFile+"\" for reading")
		}
		gr, err := gzip.NewReader(bufio.NewReader(f))
		if gr != nil {
			return nil, errors.Wrap(err, "Error creating gzip reader for temp file block.")
		}
		return &Reader{ReadCloser: gr, File: f}, nil
  case "none":
		f, err := os.OpenFile(tfb.TempFile, os.O_RDONLY, 0600)
		if err != nil {
			return nil, errors.Wrap(err, "Error opening file block at \""+tfb.TempFile+"\" for reading")
		}
    return &Reader{Reader: bufio.NewReader(f), File: f}, nil
  }
  return nil, errors.New("Unknown compression algorithm")
}

// Iterator returns a BlockIterator for iterating through the blocks data, and an error if any.
func (tfb *TempFileBlock) Iterator() (*BlockIterator, error) {
  reader, err := tfb.Reader()
  if err != nil {
    return &BlockIterator{}, errors.Wrap(err, "Error creating iterator")
  }

  it := &BlockIterator{
    Reader: reader,
    BigEndian: tfb.UseBigEndian(),
  }

  return it, nil
}

func (tfb *TempFileBlock) Get(position int) ([]byte, error) {

	it, err := tfb.Iterator()
	if err != nil {
		return make([]byte,0), errors.Wrap(err, "Error creating iterator to get bytes at position "+fmt.Sprint(position)+" in block")
	}

	err = it.Skip(position)
	if err != nil {
		return make([]byte,0), errors.Wrap(err, "Error skipping to position "+fmt.Sprint(position)+" in block \""+tfb.TempFile+"\"")
	}

	b, err := it.Next()
	if err != nil {
		return make([]byte,0), errors.Wrap(err, "Error reading position "+fmt.Sprint(position)+" in block at \""+tfb.TempFile+"\"")
	}

	err = it.Close()
	if err != nil {
		return make([]byte,0), errors.Wrap(err, "Error closing iterator for block at \""+tfb.TempFile+"\"")
	}

	return b, nil
}

// Init initializes a TempFileBlock by writing "b" to a temp file in the TempDir directory.
// The path to the temp file is saved to tfb.TempFile.
func (tfb *TempFileBlock) Init(b []byte) error {

	tempDirExpanded, err := homedir.Expand(tfb.TempDir)
	if err != nil {
		return errors.Wrap(err, "Error expanding path for file block at \""+tfb.TempDir+"\"")
	}
	tfb.TempDirExpanded = tempDirExpanded

  err = os.MkdirAll(tfb.TempDirExpanded, 0770)
	if err != nil {
		return errors.Wrap(err, "Error creating temporary directory.")
	}

	tempFile, err := ioutil.TempFile(tfb.TempDirExpanded, "go_fileblock_")
	if err != nil {
		return errors.Wrap(err, "Error creating temp file in directory \""+tfb.TempDirExpanded+"\"")
	}
	tfb.TempFile = tempFile.Name()

	w := bufio.NewWriter(tempFile)
	_, err = w.Write(b)
	if err != nil {
		return errors.Wrap(err, "Error writing bytes to file block at \""+tfb.TempDirExpanded+"\"")
	}
	err = w.Flush()
	if err != nil {
		return errors.Wrap(err, "Error flushing bytes to file block at \""+tfb.TempFile+"\"")
	}

	err = tempFile.Sync()
	if err != nil {
		return errors.Wrap(err, "Error syncing with file block at \""+tfb.TempFile+"\"")
	}

	err = tempFile.Close()
	if err != nil {
		return errors.Wrap(err, "Error closing file block at \""+tfb.TempFile+"\"")
	}

	return nil
}

// Remove removes the temp file from disk.
func (tfb *TempFileBlock) Remove() error {
	return os.Remove(tfb.TempFile)
}

// NewTempFileBlock returns a new TempFileBlock.
func NewTempFileBlock(algorithm string, bigEndian bool, tempDir string) *TempFileBlock {
  return &TempFileBlock{
		AbstractBlock: AbstractBlock{
			Algorithm: algorithm,
			BigEndian: bigEndian,
		},
		TempDir: tempDir,
  }
}
