package record

import (
	"github.com/geraldywy/cz4031_proj1/pkg/consts"
	"github.com/geraldywy/cz4031_proj1/pkg/utils"
)

// every record is a fixed at 17 bytes + 1 byte to indicate the size + storage pointer 5.
const RecordSize = 23

type Record interface {
	Serialize() []byte
	TConst() string
	AvgRating() float32
	NumVotes() int32
}

var _ Record = (*recordImpl)(nil)

func NewRecord(tconst string, avgRating float32, numVotes int32) Record {
	return &recordImpl{
		tconst:        tconst,
		averageRating: avgRating,
		numVotes:      numVotes,
	}
}

func NewRecordFromBytes(buf []byte) Record {
	return &recordImpl{
		tconst:        string(buf[1:10]),
		averageRating: utils.Float32FromBytes(utils.SliceTo4ByteArray(buf[10:14])),
		numVotes:      utils.Int32FromBytes(utils.SliceTo4ByteArray(buf[14:18])),
	}
}

type recordImpl struct {
	tconst        string // fixed size string, size 9 ascii characters only
	averageRating float32
	numVotes      int32
}

func (r *recordImpl) Serialize() []byte {
	buf := make([]byte, RecordSize)
	buf[0] = consts.RecordIdentifier
	j := 1
	for i := range r.tconst {
		buf[j] = r.tconst[i]
		j += 1
	}
	for _, b := range utils.Float32ToBytes(r.averageRating) {
		buf[j] = b
		j += 1
	}
	for _, b := range utils.Int32ToBytes(r.numVotes) {
		buf[j] = b
		j += 1
	}

	// The storage pointer bytes [18:] is to be implemented after knowing the storage pointer

	return buf
}

func (r *recordImpl) TConst() string {
	return r.tconst
}

func (r *recordImpl) AvgRating() float32 {
	return r.averageRating
}

func (r *recordImpl) NumVotes() int32 {
	return r.numVotes
}
