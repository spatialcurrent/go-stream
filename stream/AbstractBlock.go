package stream

// AbstractBlock is an abstract struct extended by MemoryBlock and TempFileBlock.
type AbstractBlock struct {
  Algorithm string         `xml:"-" json:"-"` // the compression algorithm used: snappy, gzip, or none.
  BigEndian bool `xml:"-" json:"-"` // If true, then encode numbers using a big-endian byte order, else encodes using littl-endian byte order.
}

// Returns the compress algorithm, which can be: snappy, gzip, or none.
func (ab AbstractBlock) GetAlgorithm() string {
  return ab.Algorithm
}

// UseBigEndian returns true is
func (ab AbstractBlock) UseBigEndian() bool {
  return ab.BigEndian
}
