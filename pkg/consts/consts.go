package consts

const (
	RecordIdentifier        = 0
	NodeIdentifier          = 1
	IndexedRecordIdentifier = 2
)
const StoragePtrSize = 5
const IndexedRecordSize = 1 + 2*StoragePtrSize

// every record is a fixed at 17 bytes + 1 byte to indicate the size + storage pointer 5.
const RecordSize = 19

const MB = 1048576 // 1 1MB = 2^20 Bytes
const DiskCapacity = 500 * MB
const BlockSize = 200
