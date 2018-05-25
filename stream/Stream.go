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
	"encoding"
	"encoding/binary"
	"fmt"
	"compress/gzip"
	"io"
	"io/ioutil"
)

import (
	"github.com/golang/snappy"
	"github.com/pkg/errors"
)

type Writer interface {
	io.Writer
	Flush() error
}

type WriteCloser interface {
	Writer
	io.Closer
}

type Stream struct {
	BlockType string `xml:"-" json:"-"`
	TempDir string `xml:"-" json:"-"`
	Algorithm string         `xml:"-" json:"-"`
	BigEndian bool `xml:"-" json:"-"`
	BlockSize int `xml:"-" json:"-"`
	Blocks []Block `xml:"-" json:"-"`
	Buffer    *bytes.Buffer  `xml:"-" json:"-"`
	Writer    Writer `xml:"-" json:"-"`
	WriteCloser    WriteCloser `xml:"-" json:"-"`
}

func New(alg string, endianness string, blockSize int, block_type string, tempDir string) (*Stream, error) {
	s := &Stream{
		BlockType: block_type,
		TempDir: tempDir,
		Algorithm: alg,
		BlockSize: blockSize,
		Blocks: make([]Block, 0),
	}

	if endianness == "little" {
		s.BigEndian = false
	} else if endianness == "big" {
		s.BigEndian = true
	} else {
		return s, errors.New("Invalid endianness \""+endianness+"\"")
	}
	return s, nil
}

func (s *Stream) Size() (int64, error) {
	if s.Buffer != nil {
		return int64(s.Buffer.Len()), nil
	}
	n := int64(0)
	for i, b := range s.Blocks {
		blockSize, err := b.Size()
		if err != nil {
			return 0, errors.Wrap(err, "Error calculating size for block "+fmt.Sprint(i))
		}
		n += blockSize
	}
	return n, nil
}

func (s *Stream) Init() error {
	switch s.Algorithm {
	case "snappy":
		s.Buffer = new(bytes.Buffer)
		s.WriteCloser = snappy.NewBufferedWriter(s.Buffer)
		s.Writer = s.WriteCloser
		return nil
	case "gzip":
		s.Buffer = new(bytes.Buffer)
		s.WriteCloser = gzip.NewWriter(s.Buffer)
		s.Writer = s.WriteCloser
		return nil
	case "none":
		s.Buffer = new(bytes.Buffer)
		s.Writer = bufio.NewWriter(s.Buffer)
		return nil
	}
	return errors.New("Unknown compression algorithm \"" + s.Algorithm + "\"")
}

func (s *Stream) Write(b []byte) (n int, err error) {
	return s.Writer.Write(b)
}

func (s *Stream) WriteObject(obj encoding.BinaryMarshaler) (n int, err error) {
	b, err := obj.MarshalBinary()
	if err != nil {
		return 0, errors.Wrap(err, "Error marshalling object to bytes.")
	}
	h := new(bytes.Buffer)
	if s.BigEndian {
		binary.Write(h, binary.BigEndian, uint64(len(b)))
	} else {
		binary.Write(h, binary.LittleEndian, uint64(len(b)))
	}
	n1, err := s.Write(h.Bytes())
	if err != nil {
		return n1, errors.Wrap(err, "Error writing object size to stream.")
	}
	n2, err := s.Write(b)
	if err != nil {
		return n1+n2, errors.Wrap(err, "Error writing object content to stream.")
	}
	return n1+n2, nil
}

func (s *Stream) Flush() error {
	if s.Writer != nil {
		return s.Writer.Flush()
	}
	return nil
}

func (s *Stream) Rotate() error {

	if s.Buffer == nil {
		return errors.New("Error rotating buffer to block.  Buffer is nil.")
	}

	if s.Writer != nil {
		err := s.Writer.Flush()
		if err != nil && err != io.EOF {
			return err
		}
	}

	if s.WriteCloser != nil {
		err := s.WriteCloser.Close()
		if err != nil && err != io.EOF {
			return err
		}
	}

	b, err := ioutil.ReadAll(s.Buffer)
	if err != nil {
		return errors.Wrap(err, "Error reading buffer into bytes.")
	}
	err = s.AppendBlock(b)
	if err != nil {
		return errors.Wrap(err, "Error appending new block")
	}
	//s.Blocks = append(s.Blocks, NewMemoryBlock(s.Algorithm, s.BigEndian, b))

	err = s.Init()
	if err != nil {
		return err
	}

	return nil
}

func (s *Stream) Close() error {

	if s.Writer != nil {
		err := s.Writer.Flush()
		if err != nil && err != io.EOF {
			return err
		}
	}

	if s.WriteCloser != nil {
		err := s.WriteCloser.Close()
		if err != nil && err != io.EOF {
			return err
		}
	}

	if s.Buffer != nil {
		b, err := ioutil.ReadAll(s.Buffer)
		if err != nil {
			return errors.Wrap(err, "Error reading buffer into bytes.")
		}
		err = s.AppendBlock(b)
		if err != nil {
			return errors.Wrap(err, "Error appending new block")
		}
		//s.Blocks = append(s.Blocks, NewMemoryBlock(s.Algorithm, s.BigEndian, b))
		s.Buffer = nil
	}

	return nil
}

func (s *Stream) Iterator() (*StreamIterator, error) {
	return NewStreamIterator(s.Blocks)
}

func (s *Stream) Reader(n int) (*Reader, error) {
	return s.Blocks[n].Reader()
}

// NewIterator returns a new iterator for the stream
func (s *Stream) BlockIterator(n int) (*BlockIterator, error) {
	return s.Blocks[n].Iterator()
}

func (s *Stream) Get(position int) ([]byte, error) {

	blockIndex := int(position / s.BlockSize)
	if blockIndex >= len(s.Blocks) {
		return make([]byte, 0), errors.New("Error reading from block "+fmt.Sprint(blockIndex)+".  Greater than number of blocks "+fmt.Sprint(len(s.Blocks)))
	}
	blockPosition := position % s.BlockSize
	b, err := s.Blocks[blockIndex].Get(blockPosition)
	if err != nil {
		return make([]byte, 0), errors.Wrap(err, "Error reading from block "+fmt.Sprint(blockIndex)+" at position "+fmt.Sprint(blockPosition))
	}
	return b, nil
}

func (s *Stream) AppendBlock(b []byte) error {
	var block Block
	if s.BlockType == "file" {
		block = NewTempFileBlock(s.Algorithm, s.BigEndian, s.TempDir)
	} else {
		block = NewMemoryBlock(s.Algorithm, s.BigEndian)
	}
	err := block.Init(b)
	if err != nil {
		return errors.Wrap(err, "Error initializing block.")
	}
	s.Blocks = append(s.Blocks, block)
	return nil
}

func (s *Stream) Remove() error {
	for _, b := range s.Blocks {
		b.Remove()
	}
	s.Blocks = make([]Block, 0)
	return nil
}
