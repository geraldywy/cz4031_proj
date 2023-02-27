package storage

import (
	"errors"
	"github.com/geraldywy/cz4031_proj1/pkg/consts"
	"github.com/geraldywy/cz4031_proj1/pkg/record"
	"github.com/geraldywy/cz4031_proj1/pkg/utils"
)

// Disk Storage is simulated in this package. When we say disk, we refer to a simulated in memory disk, all data is
// not persisted to the physical disk.

// All records have fixed length, no need to separate records when packing into blocks.
// Spanned - A record is potentially spread across multiple blocks, more space efficient.
// Nonsequential - Records are stored within a block, in the order of insertion, no particular ordering.

type Storable interface {
	Serialize() []byte
}

// We refer to a record stored with 2 items.
// 1. block Ptr
// 2. Record Ptr (The offset within a block)
type StoragePointer struct {
	BlockPtr  uint32
	RecordPtr uint8
}

const StoragePtrSize = 9

func (s *StoragePointer) Serialize() []byte {
	buf := make([]byte, StoragePtrSize)
	j := 0
	for _, b := range utils.UInt32ToBytes(s.BlockPtr) {
		buf[j] = b
		j += 1
	}
	buf[j] = utils.UInt32ToBytes(uint32(s.RecordPtr))[0]

	return buf
}

func NewStoragePointerFromBytes(buf []byte) *StoragePointer {
	ptr := &StoragePointer{
		BlockPtr:  utils.UInt32FromBytes(utils.SliceTo4ByteArray(buf[:4])),
		RecordPtr: buf[4],
	}
	if ptr.BlockPtr == 0 && ptr.RecordPtr == 0 {
		return nil
	}

	return ptr
}

// ttconst -> storage pointer
type StoragePointers map[string]*StoragePointer

type Storage interface {
	// internal methods
	insert(item Storable, prev *StoragePointer) (*StoragePointer, error)
	read(ptr *StoragePointer) ([]byte, error)
	delete(delPtr *StoragePointer, newPtr *StoragePointer, delPrev *StoragePointer) error
	copy(ptr *StoragePointer, buf []byte, isDel bool) error

	// InsertRecord stores a new record on "disk", returns the offset to read the data.
	InsertRecord(record record.Record) (*StoragePointer, error)
	// ReadRecord returns a record given a pointer to start reading from.
	ReadRecord(ptr *StoragePointer) (record.Record, error)
	// DeleteRecord deletes a record given a pointer. This is an O(1) operation.
	DeleteRecord(ptr *StoragePointer) error
}

var _ Storage = (*storageImpl)(nil)

type storageImpl struct {
	// We simulate the disk storage as a contiguous one, using a slice of bytes.
	store []block

	// below fields are in terms of bytes
	spaceUsed   int
	maxCapacity int
	blockSize   int

	lastRecordInsertedPtr *StoragePointer
	lastNodeInsertedPtr   *StoragePointer

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
		lastRecordInsertedPtr: nil,
		lastNodeInsertedPtr:   nil,
		avgRatingIndexing:     false,
		avgRatingIndexMapping: make(map[float32]StoragePointers),
		numVotesIndexing:      false,
		numVotesIndexMapping:  make(map[int32]StoragePointers),
	}
}

// A block is nothing more than a series of bytes.
type block []byte

const (
	// block first byte = Space used, stored as an int4, since block size = 200B, 2^8 = 256, 1 byte is sufficient
	// consequently, number of records stored = space used / record size
	blockHeaderSize = 1
)

func newBlock(size int) block {
	blk := make([]byte, size)
	blk[0] = blockHeaderSize

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

func (b *block) spaceUsed() uint8 {
	if len(*b) < blockHeaderSize {
		return 0
	}

	return (*b)[0]
}

func (b *block) updateSize(newSize uint8) {
	if len(*b) < blockHeaderSize {
		return
	}

	(*b)[0] = newSize
}

var (
	ErrBlockNotExist           = errors.New("requested block does not exist")
	ErrRecordNotExist          = errors.New("requested record does not exist")
	ErrReadNotExist            = errors.New("bad read")
	ErrInsufficientSpaceOnDisk = errors.New("disk is full")
	ErrBadWrite                = errors.New("bad write")
)

func (s *storageImpl) insert(item Storable, prev *StoragePointer) (*StoragePointer, error) {
	buf := item.Serialize()
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
		BlockPtr:  uint32(len(s.store) - 1),
		RecordPtr: j,
	}
	// write back prev storage pointer location
	if prev != nil {
		x := len(buf) - StoragePtrSize
		for _, b := range prev.Serialize() {
			buf[x] = b
			x++
		}
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

func (s *storageImpl) InsertRecord(record record.Record) (*StoragePointer, error) {
	ptr, err := s.insert(record, s.lastRecordInsertedPtr)
	if err != nil {
		return nil, err
	}
	s.lastRecordInsertedPtr = ptr

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

func (s *storageImpl) read(ptr *StoragePointer) ([]byte, error) {
	if ptr == nil {
		return nil, nil
	}
	if ptr.BlockPtr >= uint32(len(s.store)) {
		return nil, ErrBlockNotExist
	}

	size := uint64(record.RecordSize)
	if s.store[ptr.BlockPtr][ptr.RecordPtr] == consts.NodeIdentifier {
		bptr := ptr.BlockPtr
		rptr := ptr.RecordPtr + 1
		if rptr == uint8(s.blockSize) {
			bptr++
			rptr = blockHeaderSize
		}
		m := uint64(s.store[bptr][rptr]) // number of keys in a node
		size = 1 + m*14 + 9 + 9          // 1 byte for identifier, m * (sPtr 9 + valType 1 + value 4) + last sPtr + node sPtr
	}

	buf := make([]byte, size)
	var blockOffset uint32
	var i int
	recordStart := ptr.RecordPtr
	for i < len(buf) {
		blk := s.store[ptr.BlockPtr+blockOffset]
		t := buf[i:]
		cnt := blk.read(int(recordStart), int(recordStart)+len(buf)-i, &t)
		if cnt == 0 {
			return nil, ErrReadNotExist
		}
		i += cnt
		blockOffset++
		recordStart = blockHeaderSize
	}

	return buf, nil
}

func (s *storageImpl) ReadRecord(ptr *StoragePointer) (record.Record, error) {
	buf, err := s.read(ptr)
	if err != nil {
		return nil, err
	}

	return record.NewRecordFromBytes(buf), nil
}

func (s *storageImpl) delete(delPtr, lastPtr, delPrev *StoragePointer) error {
	if _, err := s.read(delPtr); err != nil {
		return err
	}
	last, err := s.read(lastPtr)
	if err != nil {
		return err
	}
	var prevPrevPtrBuf []byte
	if delPrev == nil {
		prevPrevPtrBuf = make([]byte, StoragePtrSize)
	} else {
		prevPrevPtrBuf = delPrev.Serialize()
	}
	x := len(last) - StoragePtrSize
	for _, b := range prevPrevPtrBuf {
		last[x] = b
		x += 1
	}
	if err := s.copy(delPtr, last, false); err != nil {
		return err
	}
	tmp := make([]byte, len(last))
	if err := s.copy(lastPtr, tmp, true); err != nil {
		return err
	}

	// free up empty blocks
	for s.store[len(s.store)-1].spaceUsed() == blockHeaderSize {
		s.store = s.store[:len(s.store)-1]
		s.spaceUsed -= s.blockSize
	}

	return nil
}

func (s *storageImpl) DeleteRecord(ptr *StoragePointer) error {
	delBuf, err := s.read(ptr)
	if err != nil {
		return err
	}
	delRecord := record.NewRecordFromBytes(delBuf)

	buf, err := s.read(s.lastRecordInsertedPtr)
	if err != nil {
		return err
	}
	lastRecord := record.NewRecordFromBytes(buf)

	delPrev := NewStoragePointerFromBytes(delBuf[len(delBuf)-StoragePtrSize:])
	prevPrevPtr := NewStoragePointerFromBytes(buf[len(buf)-StoragePtrSize:])
	s.delete(ptr, s.lastRecordInsertedPtr, delPrev)
	s.lastRecordInsertedPtr = prevPrevPtr

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

	return nil
}

func (s *storageImpl) copy(dst *StoragePointer, buf []byte, isDel bool) error {
	var blockOffset uint32
	writePtr := int(dst.RecordPtr)

	var i int
	for i < len(buf) {
		blk := s.store[dst.BlockPtr+blockOffset]
		cnt := blk.write(writePtr, writePtr+len(buf)-i, buf[i:])
		if cnt == 0 {
			return ErrBadWrite
		}
		if isDel {
			blk.updateSize(blk.spaceUsed() - uint8(cnt))
		}

		i += cnt
		blockOffset++
		writePtr = blockHeaderSize
	}
	return nil
}
