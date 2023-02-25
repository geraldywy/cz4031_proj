package record

import (
	"github.com/geraldywy/cz4031_proj1/pkg/utils"
)

type SerializedRecord [17]byte

type Record interface {
	TConst() string
	AvgRating() float32
	NumVotes() int32
	Serialize() SerializedRecord
}

var _ Record = (*recordImpl)(nil)

func NewRecord(tconst string, avgRating float32, numVotes int32) Record {
	return &recordImpl{
		tconst:        tconst,
		averageRating: avgRating,
		numVotes:      numVotes,
	}
}

func NewRecordFromBytes(buf SerializedRecord) Record {
	return &recordImpl{
		tconst:        string(buf[:9]),
		averageRating: utils.Float32FromBytes(utils.SliceTo4ByteArray(buf[9:13])),
		numVotes:      utils.Int32FromBytes(utils.SliceTo4ByteArray(buf[13:])),
	}
}

type recordImpl struct {
	tconst        string // fixed size string, size 7 ascii characters only
	averageRating float32
	numVotes      int32
}

func (r *recordImpl) Serialize() SerializedRecord {
	var buf SerializedRecord
	j := 0
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
