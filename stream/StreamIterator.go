// =================================================================
//
// Copyright (C) 2018 Spatial Current, Inc. - All Rights Reserved
// Released as open source under the MIT License.  See LICENSE file.
//
// =================================================================

package stream

import (
  "io"
)

import (
  "github.com/pkg/errors"
)

type StreamIterator struct {
  Blocks []Block `xml:"-" json:"-"`
  BlockIndex int `xml:"-" json:"-"`
  BlockIterator *BlockIterator `xml:"-" json:"-"`
}

func NewStreamIterator(blocks []Block) (*StreamIterator, error) {
  if len(blocks) == 0 {
    return &StreamIterator{}, errors.New("Invalid number of blocks.  Need at least 1 block.")
  }

  bi, err := blocks[0].Iterator()
  if err != nil {
    return &StreamIterator{}, errors.Wrap(err, "Error creating stream iterator")
  }

  si := &StreamIterator{
    Blocks: blocks,
    BlockIndex: 0,
    BlockIterator: bi,
  }

  return si, nil
}

func (si *StreamIterator) Next() ([]byte, error) {
  b, err := si.BlockIterator.Next()
  if err != nil {
    if err == io.EOF {
      if si.BlockIndex < len(si.Blocks) - 1 {
        si.BlockIndex += 1
        bi, err := si.Blocks[si.BlockIndex].Iterator()
        if err != nil {
          return make([]byte, 0), errors.Wrap(err, "Error creating stream iterator")
        }
        si.BlockIterator = bi
        return si.Next()
      }
    }
  }
  return b, err
}
