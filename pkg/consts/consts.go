package consts

import "github.com/geraldywy/cz4031_proj1/pkg/storage_ptr"

const (
	RecordIdentifier        = 0
	NodeIdentifier          = 1
	IndexedRecordIdentifier = 2
)

const IndexedRecordSize = 1 + 2*storage_ptr.StoragePtrSize

// every record is a fixed at 17 bytes + 1 byte to indicate the size + storage pointer 5.
const RecordSize = 18
