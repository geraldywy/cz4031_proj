package bptree

import "github.com/geraldywy/cz4031_proj1/pkg/record"

type BPTree interface {
	Insert(key uint32, rec record.Record) error
	//Search(key uint32) ([]record.Record, error)
	//// DeleteAll deletes all indexes with this key
	//DeleteAll(key uint32) error
	//SearchRange(from uint32, to uint32) ([]record.Record, error)
}
