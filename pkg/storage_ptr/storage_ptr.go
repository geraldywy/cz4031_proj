package storage_ptr

import (
	"github.com/geraldywy/cz4031_proj1/pkg/consts"
	"github.com/geraldywy/cz4031_proj1/pkg/utils"
)

// We refer to a record stored with 2 items.
// 1. block Ptr
// 2. Record Ptr (The offset within a block)
type StoragePointer struct {
	BlockPtr  uint32
	RecordPtr uint8
}

func (s *StoragePointer) Serialize() []byte {
	buf := make([]byte, consts.StoragePtrSize)
	if s == nil {
		return buf
	}
	j := 0
	for _, b := range utils.UInt32ToBytes(s.BlockPtr) {
		buf[j] = b
		j += 1
	}
	buf[j] = utils.UInt32ToBytes(uint32(s.RecordPtr))[3]

	return buf
}

func (s *StoragePointer) DeepEqual(other *StoragePointer) bool {
	if s == nil || other == nil {
		return s == other
	}

	return s.BlockPtr == other.BlockPtr && s.RecordPtr == other.RecordPtr
}

func NewStoragePointerFromBytes(buf []byte) *StoragePointer {
	if buf == nil {
		return nil
	}
	ptr := &StoragePointer{
		BlockPtr:  utils.UInt32FromBytes(utils.SliceTo4ByteArray(buf[:4])),
		RecordPtr: buf[4],
	}
	if ptr.BlockPtr == 0 && ptr.RecordPtr == 0 {
		return nil
	}

	return ptr
}
