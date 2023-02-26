package storage

import (
	"errors"
	"github.com/geraldywy/cz4031_proj1/pkg/record"
	"github.com/geraldywy/cz4031_proj1/pkg/utils"
)

// Disk Storage is simulated in this package. When we say disk, we refer to a simulated in memory disk, all data is
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

// ttconst -> storage pointer
type StoragePointers map[string]*StoragePointer

type Storage interface {
	// InsertRecord stores a new record on "disk", returns the offset to read the data.
	InsertRecord(record record.Record) (*StoragePointer, error)
	// ReadRecord returns a record given a pointer to start reading from.
	ReadRecord(ptr *StoragePointer) (record.Record, error)
	// DeleteRecord deletes a record given a pointer. This is an O(1) operation.
	DeleteRecord(ptr *StoragePointer) error
	// UpdateRecord updates a record given a pointer, also indicate if the update is used to write 0 byte to update block space used book-keeping
	UpdateRecord(ptr *StoragePointer, newRecord record.Record, isDel bool) error
}

var _ Storage = (*storageImpl)(nil)

type storageImpl struct {
	// We simulate the disk storage as a contiguous one, using a slice of bytes.
	store []block

	// below fields are in terms of bytes
	spaceUsed   int
	maxCapacity int
	blockSize   int

	// indexing
	avgRatingIndexing     bool
	avgRatingIndexMapping map[float32]StoragePointers

	numVotesIndexing     bool
	numVotesIndexMapping map[int32]StoragePointers
}

func NewStorage(maxCapacity int, blockSize int) Storage {
	return &storageImpl{
		store:                 make([]block, 0),
		spaceUsed:             0,
		maxCapacity:           maxCapacity,
		blockSize:             blockSize,
		avgRatingIndexing:     false,
		avgRatingIndexMapping: make(map[float32]StoragePointers),
		numVotesIndexing:      false,
		numVotesIndexMapping:  make(map[int32]StoragePointers),
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

func (b *block) write(from, till int, buf []byte) int {
	if buf == nil {
		return 0
	}
	i := 0
	for from < till && from < len(*b) {
		(*b)[from] = buf[i]
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

	if s.numVotesIndexing {
		if s.numVotesIndexMapping[record.NumVotes()] == nil {
			s.numVotesIndexMapping[record.NumVotes()] = make(map[string]*StoragePointer)
		}
		s.numVotesIndexMapping[record.NumVotes()][record.TConst()] = ptr
	}
	if s.avgRatingIndexing {
		if s.avgRatingIndexMapping[record.AvgRating()] == nil {
			s.avgRatingIndexMapping[record.AvgRating()] = make(map[string]*StoragePointer)
		}
		s.avgRatingIndexMapping[record.AvgRating()][record.TConst()] = ptr
	}

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

func (s *storageImpl) DeleteRecord(ptr *StoragePointer) error {
	if ptr.BlockPtr >= len(s.store) || len(s.store) == 0 {
		return ErrBlockNotExist
	}

	// get head of last record
	lastBlkIdx := len(s.store) - 1
	lastBlk := s.store[lastBlkIdx]
	lastRecordStart := lastBlk.spaceUsed() - record.RecordSize
	if lastRecordStart < 4 {
		if lastRecordStart < 0 {
			lastRecordStart = -lastRecordStart + 4
		}
		lastBlkIdx = len(s.store) - 2
		lastBlk = s.store[lastBlkIdx]
		lastRecordStart = lastBlk.spaceUsed() - lastRecordStart
	}

	delRecord, err := s.ReadRecord(ptr)
	if err != nil {
		return err
	}
	lastPtr := &StoragePointer{
		BlockPtr:  lastBlkIdx,
		RecordPtr: int(lastRecordStart),
	}
	lastRecord, err := s.ReadRecord(lastPtr)
	if err != nil {
		return err
	}
	// update indexing, remove index for del record, update index for lastRecord
	// note: order matters, in the case where we are deleting the only record
	// we first insert old, then delete new so that if new = old, deletion still occurs
	if s.numVotesIndexing {
		s.numVotesIndexMapping[lastRecord.NumVotes()][lastRecord.TConst()] = ptr
		delete(s.numVotesIndexMapping[delRecord.NumVotes()], delRecord.TConst())
		if len(s.numVotesIndexMapping[delRecord.NumVotes()]) == 0 {
			delete(s.numVotesIndexMapping, delRecord.NumVotes())
		}
	}
	if s.avgRatingIndexing {
		s.avgRatingIndexMapping[lastRecord.AvgRating()][lastRecord.TConst()] = ptr
		delete(s.avgRatingIndexMapping[delRecord.AvgRating()], delRecord.TConst())
		if len(s.avgRatingIndexMapping[delRecord.AvgRating()]) == 0 {
			delete(s.avgRatingIndexMapping, delRecord.AvgRating())
		}
	}

	if err := s.UpdateRecord(ptr, lastRecord, false); err != nil {
		return err
	}
	if err := s.UpdateRecord(lastPtr, record.NewRecordFromBytes(record.SerializedRecord{}), true); err != nil {
		return err
	}

	return nil
}

// UpdateRecord updates a record byte with the new content.
func (s *storageImpl) UpdateRecord(ptr *StoragePointer, newRecord record.Record, isDel bool) error {
	// record must exist first
	if _, err := s.ReadRecord(ptr); err != nil {
		return err
	}

	var i, blockOffset int
	buf := newRecord.Serialize()
	writePtr := ptr.RecordPtr
	for i < len(buf) {
		blk := s.store[ptr.BlockPtr+blockOffset]
		cnt := blk.write(writePtr, writePtr+len(buf)-i, buf[i:])
		if cnt == 0 {
			return ErrRecordNotExist
		}
		if isDel {
			blk.updateSize(blk.spaceUsed() - int32(cnt))
		}
		i += cnt
		blockOffset++
		writePtr = blockHeaderSize
	}
	return nil
}
