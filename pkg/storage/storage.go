package storage

import (
	"errors"
	"github.com/geraldywy/cz4031_proj1/pkg/record"
	"github.com/geraldywy/cz4031_proj1/pkg/utils"
)

// Disk Storage is simulated in this package. When we say disk, we refer to the in memory disk, all data is
// not persisted to the physical disk.

// All records have fixed length, no need to separate records when packing into blocks.
// Spanned - A record is potentially spread across multiple blocks, more space efficient.
// Nonsequential - Records are stored within a block, in the order of insertion, no particular ordering.

// We refer to a record stored with 2 items.
// 1. block Ptr
// 2. Record Ptr (The offset within a block)
type StoragePointer struct {
	BlockPtr  int
	RecordPtr int
}

type Storage interface {
	// InsertRecord stores a new record on "disk", returns the offset to read the data.
	InsertRecord(record record.Record) (*StoragePointer, error)
	// ReadRecord returns a record given a pointer to start reading from.
	ReadRecord(ptr *StoragePointer) (record.Record, error)
	//// DeleteRecord deletes a record given a pointer. This is an O(1) operation.
	//DeleteRecord(ptr StoragePointer) error
	//// UpdateRecord updates a record given a pointer.
	//UpdateRecord(ptr StoragePointer) error
}

var _ Storage = (*storageImpl)(nil)

type storageImpl struct {
	// We simulate the disk storage as a contiguous one, using a slice of bytes.
	store []block

	// below fields are in terms of bytes
	spaceUsed   int
	maxCapacity int
	blockSize   int
}

func NewStorage(maxCapacity int, blockSize int) Storage {
	return &storageImpl{
		store:       make([]block, 0),
		spaceUsed:   0,
		maxCapacity: maxCapacity,
		blockSize:   blockSize,
	}
}

// A block is nothing more than a series of bytes.
type block []byte

const (
	// block first 4 bytes = Space used, stored as an int32.
	// consequently, number of records stored = space used / record size
	blockHeaderSize = 4
)

func newBlock(size int) block {
	blk := make([]byte, size)
	for i, v := range utils.Int32ToBytes(4) {
		blk[i] = v
	}

	return blk
}

// populates buf with block content from ptr "from" up till before (non inclusive) "to", returns the total number of bytes read
func (b *block) read(from, till int, buf *[]byte) int {
	if buf == nil {
		return 0
	}
	i := 0
	for from < till && from < len(*b) {
		(*buf)[i] = (*b)[from]
		i++
		from++
	}

	return i
}

func (b *block) spaceUsed() int32 {
	if len(*b) < 4 {
		return -1
	}
	var buf [4]byte
	copy(buf[:], (*b)[:4])
	return utils.Int32FromBytes(buf)
}

func (b *block) updateSize(newSize int32) {
	if len(*b) < 4 {
		return
	}

	for i, v := range utils.Int32ToBytes(newSize) {
		(*b)[i] = v
	}
}

var (
	ErrBlockNotExist           = errors.New("requested block does not exist")
	ErrRecordNotExist          = errors.New("requested record does not exist")
	ErrInsufficientSpaceOnDisk = errors.New("disk is full")
)

func (s *storageImpl) InsertRecord(record record.Record) (*StoragePointer, error) {
	buf := record.Serialize()
	if len(s.store) == 0 || int(s.store[len(s.store)-1].spaceUsed()) == s.blockSize {
		if s.spaceUsed+utils.Max(s.blockSize, len(buf)) > s.maxCapacity {
			return nil, ErrInsufficientSpaceOnDisk
		}

		s.store = append(s.store, newBlock(s.blockSize))
		s.spaceUsed += s.blockSize
	}

	lastBlk := s.store[len(s.store)-1]
	j := lastBlk.spaceUsed()
	ptr := &StoragePointer{
		BlockPtr:  len(s.store) - 1,
		RecordPtr: int(j),
	}
	for _, v := range buf {
		if int(j) == s.blockSize {
			lastBlk.updateSize(j)
			s.store = append(s.store, newBlock(s.blockSize))
			s.spaceUsed += s.blockSize
			lastBlk = s.store[len(s.store)-1]
			j = lastBlk.spaceUsed()
		}
		lastBlk[j] = v
		j++
	}
	lastBlk.updateSize(j)

	return ptr, nil
}

func (s *storageImpl) ReadRecord(ptr *StoragePointer) (record.Record, error) {
	if ptr.BlockPtr >= len(s.store) {
		return nil, ErrBlockNotExist
	}

	var buf record.SerializedRecord
	var i, blockOffset int
	recordStart := ptr.RecordPtr
	for i < len(buf) {
		blk := s.store[ptr.BlockPtr+blockOffset]
		t := buf[i:]
		cnt := blk.read(recordStart, recordStart+len(buf)-i, &t)
		if cnt == 0 {
			return nil, ErrRecordNotExist
		}
		i += cnt
		blockOffset++
		recordStart = blockHeaderSize
	}
	return record.NewRecordFromBytes(buf), nil
}
