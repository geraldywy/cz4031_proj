package storage

import (
	"errors"
	"github.com/geraldywy/cz4031_proj1/pkg/consts"
	"github.com/geraldywy/cz4031_proj1/pkg/record"
	"github.com/geraldywy/cz4031_proj1/pkg/storage_ptr"
	"github.com/geraldywy/cz4031_proj1/pkg/utils"
)

type Storable interface {
	Serialize() []byte
}

type Storage interface {
	//// internal methods
	//insert(item Storable) (*storage_ptr.StoragePointer, error)
	//read(ptr *storage_ptr.StoragePointer) ([]byte, error)
	//delete(delPtr *storage_ptr.StoragePointer, newPtr *storage_ptr.StoragePointer, delPrev *storage_ptr.StoragePointer) error
	Copy(ptr *storage_ptr.StoragePointer, buf []byte, isDel bool) error

	// InsertRecord stores a new record on "disk", returns the offset to read the data.
	InsertRecord(record record.Record) (*storage_ptr.StoragePointer, error)
	// ReadRecord returns a record given a pointer to start reading from.
	ReadRecord(ptr *storage_ptr.StoragePointer) (record.Record, error)
	// DeleteRecord deletes a record given a pointer. This is an O(1) operation.
	DeleteRecord(ptr *storage_ptr.StoragePointer) error

	InsertBPTNode(node *BPTNode) (*storage_ptr.StoragePointer, error)
	UpdateBPTNode(ptr *storage_ptr.StoragePointer, updatedNode *BPTNode) error
	DeleteBPTNode(ptr *storage_ptr.StoragePointer) error
	ReadBPTNode(ptr *storage_ptr.StoragePointer) (*BPTNode, error)
}

// Disk Storage is simulated in this package. When we say disk, we refer to a simulated in memory disk, all data is
// not persisted to the physical disk.

// All records have fixed length, no need to separate records when packing into blocks.
// Spanned - A record is potentially spread across multiple blocks, more space efficient.
// Nonsequential - Records are stored within a block, in the order of insertion, no particular ordering.

var _ Storable = (*storage_ptr.StoragePointer)(nil)

var _ Storage = (*storageImpl)(nil)

type storageImpl struct {
	// We simulate the disk storage as a contiguous one, using a slice of bytes.
	store []block

	// below fields are in terms of bytes
	spaceUsed   int
	maxCapacity int
	blockSize   int

	lastRecordInsertedPtr *storage_ptr.StoragePointer
	lastBPTNodeInserted   *storage_ptr.StoragePointer
}

func NewStorage(maxCapacity int, blockSize int) Storage {
	return &storageImpl{
		store:                 make([]block, 0),
		spaceUsed:             0,
		maxCapacity:           maxCapacity,
		blockSize:             blockSize,
		lastRecordInsertedPtr: nil,
		lastBPTNodeInserted:   nil,
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
	ErrUnrecognisedDiskData    = errors.New("unrecognised identifying byte")
)

func (s *storageImpl) insert(item Storable) (*storage_ptr.StoragePointer, error) {
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
	ptr := &storage_ptr.StoragePointer{
		BlockPtr:  uint32(len(s.store) - 1),
		RecordPtr: j,
	}
	// write back prev storage pointer location
	if s.lastRecordInsertedPtr != nil {
		x := len(buf) - storage_ptr.StoragePtrSize
		for _, b := range s.lastRecordInsertedPtr.Serialize() {
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

func (s *storageImpl) InsertRecord(record record.Record) (*storage_ptr.StoragePointer, error) {
	ptr, err := s.insert(record)
	if err != nil {
		return nil, err
	}
	s.lastRecordInsertedPtr = ptr

	return ptr, nil
}

func (s *storageImpl) read(ptr *storage_ptr.StoragePointer) ([]byte, error) {
	if ptr == nil {
		return nil, nil
	}
	if ptr.BlockPtr >= uint32(len(s.store)) {
		return nil, ErrBlockNotExist
	}

	var size int
	switch s.store[ptr.BlockPtr][ptr.RecordPtr] {
	case consts.RecordIdentifier:
		size = record.RecordSize
	case consts.NodeIdentifier:
		bptr := ptr.BlockPtr
		rptr := ptr.RecordPtr + 1
		if rptr == uint8(s.blockSize) {
			bptr++
			rptr = blockHeaderSize
		}
		m := int(s.store[bptr][rptr])                                                      // number of keys in a node
		size = 1 + 1 + 1 + m*(storage_ptr.StoragePtrSize+4) + storage_ptr.StoragePtrSize*2 // ident, m_val, is_leaf, m*(storage_ptr + val) + storage_ptr + storage_ptr
	case consts.IndexedRecordIdentifier:
		size = consts.IndexedRecordSize
	default:
		return nil, ErrUnrecognisedDiskData
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

func (s *storageImpl) ReadRecord(ptr *storage_ptr.StoragePointer) (record.Record, error) {
	buf, err := s.read(ptr)
	if err != nil {
		return nil, err
	}

	return record.NewRecordFromBytes(buf), nil
}

func (s *storageImpl) delete(delPtr, lastPtr, delPrev *storage_ptr.StoragePointer) error {
	if _, err := s.read(delPtr); err != nil {
		return err
	}
	last, err := s.read(lastPtr)
	if err != nil {
		return err
	}
	var prevPrevPtrBuf []byte
	if delPrev == nil {
		prevPrevPtrBuf = make([]byte, storage_ptr.StoragePtrSize)
	} else {
		prevPrevPtrBuf = delPrev.Serialize()
	}
	x := len(last) - storage_ptr.StoragePtrSize
	for _, b := range prevPrevPtrBuf {
		last[x] = b
		x += 1
	}
	if err := s.Copy(delPtr, last, false); err != nil {
		return err
	}
	tmp := make([]byte, len(last))
	if err := s.Copy(lastPtr, tmp, true); err != nil {
		return err
	}

	// free up empty blocks
	for s.store[len(s.store)-1].spaceUsed() == blockHeaderSize {
		s.store = s.store[:len(s.store)-1]
		s.spaceUsed -= s.blockSize
	}

	return nil
}

func (s *storageImpl) DeleteRecord(delPtr *storage_ptr.StoragePointer) error {
	delBuf, err := s.read(delPtr)
	if err != nil {
		return err
	}

	buf, err := s.read(s.lastRecordInsertedPtr)
	if err != nil {
		return err
	}

	delPrev := storage_ptr.NewStoragePointerFromBytes(delBuf[len(delBuf)-storage_ptr.StoragePtrSize:])
	prevPrevPtr := storage_ptr.NewStoragePointerFromBytes(buf[len(buf)-storage_ptr.StoragePtrSize:])
	s.delete(delPtr, s.lastRecordInsertedPtr, delPrev)
	s.lastRecordInsertedPtr = prevPrevPtr

	return nil
}

func (s *storageImpl) Copy(dst *storage_ptr.StoragePointer, buf []byte, isDel bool) error {
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

func (s *storageImpl) InsertBPTNode(node *BPTNode) (*storage_ptr.StoragePointer, error) {
	ptr, err := s.insert(node)
	if err != nil {
		return nil, err
	}
	s.lastBPTNodeInserted = ptr

	return ptr, nil
}

func (s *storageImpl) ReadBPTNode(ptr *storage_ptr.StoragePointer) (*BPTNode, error) {
	buf, err := s.read(ptr)
	if err != nil {
		return nil, err
	}

	return NewBPTNodeFromBytes(buf), nil
}

func (s *storageImpl) UpdateBPTNode(ptr *storage_ptr.StoragePointer, updatedNode *BPTNode) error {
	//TODO implement me
	panic("implement me")
}

func (s *storageImpl) DeleteBPTNode(ptr *storage_ptr.StoragePointer) error {
	//TODO implement me
	panic("implement me")
}

type BPTreeNode interface {
	Storable
	IsLeafNode() bool
}

var _ BPTreeNode = (*BPTNode)(nil)

type BPTNode struct {
	m         uint8                         // number of child
	keys      []uint32                      // size m
	childPtrs []*storage_ptr.StoragePointer // size m+1

	isLeafNode bool
}

// adds a key into this node
func (b *BPTNode) AddKey(key uint32, rec record.Record, store Storage) error {
	//TODO implement me
	panic("implement me")
}

//func (b *BPTNode) Search(key uint32) ([]record.Record, error) {
//	//TODO implement me
//	panic("implement me")
//}
//
//func (b *BPTNode) DeleteAll(key uint32) error {
//	//TODO implement me
//	panic("implement me")
//}
//
//func (b *BPTNode) SearchRange(from uint32, to uint32) ([]record.Record, error) {
//	//TODO implement me
//	panic("implement me")
//}

func (b *BPTNode) Serialize() []byte {
	buf := make([]byte, 0)
	buf = append(buf, consts.NodeIdentifier)
	buf = append(buf, b.m)
	isLeafByte := uint8(0)
	if b.isLeafNode {
		isLeafByte = 1
	}
	buf = append(buf, isLeafByte)
	buf = append(buf, b.childPtrs[0].Serialize()...)
	for i := range b.keys {
		for _, x := range utils.UInt32ToBytes(b.keys[i]) {
			buf = append(buf, x)
		}
		buf = append(buf, b.childPtrs[i+1].Serialize()...)
	}
	for i := 0; i < storage_ptr.StoragePtrSize; i++ {
		buf = append(buf, 0)
	}

	return buf
}

func NewBPTNode(m uint8, isLeaf bool) *BPTNode {
	// a key is identified to be in use with the following logic.
	// if the node is an internal node, the key is surrounded by 2 non nil ptrs
	// if the node is a leaf node, the key's left ptr is non nil

	return &BPTNode{
		m:          m,
		keys:       make([]uint32, m),
		childPtrs:  make([]*storage_ptr.StoragePointer, m+1),
		isLeafNode: isLeaf,
	}
}

func NewBPTNodeFromBytes(buf []byte) *BPTNode {
	isLeaf := false
	if buf[2] == 1 {
		isLeaf = true
	}
	m := buf[1]
	children := make([]*storage_ptr.StoragePointer, 0)
	keys := make([]uint32, 0)
	for i := 0; i < int(m); i++ {
		pkt := buf[3+i*(storage_ptr.StoragePtrSize+4) : 3+(i+1)*(storage_ptr.StoragePtrSize+4)]
		children = append(children, storage_ptr.NewStoragePointerFromBytes(pkt[:storage_ptr.StoragePtrSize]))
		keys = append(keys, utils.UInt32FromBytes(utils.SliceTo4ByteArray(pkt[storage_ptr.StoragePtrSize:])))
	}
	children = append(children,
		storage_ptr.NewStoragePointerFromBytes(buf[3+m*(storage_ptr.StoragePtrSize+4):3+(m+1)*(storage_ptr.StoragePtrSize+4)]))
	return &BPTNode{
		m:          m,
		keys:       keys,
		childPtrs:  children,
		isLeafNode: isLeaf,
	}
}

func (b *BPTNode) IsLeafNode() bool {
	return b.isLeafNode
}

type IndexedRecord struct {
	recordPtr *storage_ptr.StoragePointer
	nxtPtr    *storage_ptr.StoragePointer
}

func (ir *IndexedRecord) Serialize() []byte {
	buf := make([]byte, 0)
	buf = append(buf, consts.IndexedRecordIdentifier)
	buf = append(buf, ir.recordPtr.Serialize()...)
	buf = append(buf, ir.nxtPtr.Serialize()...)
	return buf
}

func (ir *IndexedRecord) IsLeafNode() bool {
	return false
}
