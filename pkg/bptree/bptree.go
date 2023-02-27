package bptree

import (
	"errors"
	"github.com/geraldywy/cz4031_proj1/pkg/storage"
	"github.com/geraldywy/cz4031_proj1/pkg/storage_ptr"
	"math"
)

var (
	ErrKeyNotFound = errors.New("key not found in tree")
)

type BPTree interface {
	// Search returns the leaf node where the key should be in.
	Search(key uint32) (*storage.BPTNode, *storage_ptr.StoragePointer, error)
	Insert(key uint32, ptr *storage_ptr.StoragePointer) error
	//// DeleteAll deletes all indexes with this key
	//DeleteAll(key uint32) error
	//SearchRange(from uint32, to uint32) ([]record.Record, error)
}

var _ BPTree = (*bptree)(nil)

type bptree struct {
	m           uint8
	store       storage.Storage
	rootNode    *storage.BPTNode
	rootNodePtr *storage_ptr.StoragePointer
}

func NewBPTree(m uint8, store storage.Storage) (BPTree, error) {
	root := storage.NewBPTNode(m, true)
	ptr, err := store.Insert(root)
	if err != nil {
		return nil, err
	}
	return &bptree{m: m, store: store, rootNode: root, rootNodePtr: ptr}, nil
}

func (b *bptree) Search(key uint32) (*storage.BPTNode, *storage_ptr.StoragePointer, error) {
	if b.rootNode == nil {
		return nil, nil, ErrKeyNotFound
	}
	tmp := b.rootNode
	tmpPtr := b.rootNodePtr
	for !tmp.IsLeafNode {
		keys := tmp.Keys
		// TODO: maybe change linear to binary search
		var idx int
		for i := range append(keys, math.MaxUint32) {
			if tmp.ChildPtrs[i+1] == nil || key < keys[i] {
				break
			}
			idx = i
		}

		tmpPtr = tmp.ChildPtrs[idx]
		buf, err := b.store.Read(tmp.ChildPtrs[idx])
		if err != nil {
			return nil, nil, err
		}
		tmp = storage.NewBPTNodeFromBytes(buf)
	}

	return tmp, tmpPtr, nil
}

func (b *bptree) Insert(key uint32, ptr *storage_ptr.StoragePointer) error {
	node, nodePtr, err := b.Search(key)
	if err != nil {
		return err
	}

	idx := -1
	for i, v := range node.Keys {
		if key == v && node.ChildPtrs[i] != nil {
			idx = i
			break
		}
	}

	// support duplicate keys
	if idx != -1 {
		lastPtr := node.ChildPtrs[idx]
		buf, err := b.store.Read(lastPtr)
		if err != nil {
			return err
		}
		idxedRec := storage.IndexedRecordFromBytes(buf)
		for idxedRec.NxtPtr != nil {
			lastPtr = idxedRec.NxtPtr
			buf, err := b.store.Read(lastPtr)
			if err != nil {
				return err
			}
			idxedRec = storage.IndexedRecordFromBytes(buf)
		}
		newPtr, err := b.store.Insert(&storage.IndexedRecord{
			RecordPtr: ptr,
			NxtPtr:    nil,
		})
		if err != nil {
			return err
		}
		idxedRec.NxtPtr = newPtr
		return b.store.Update(lastPtr, idxedRec.Serialize())
	}

	// need to insert a new key into the tree
	keys := make([]uint32, 0)
	v2s := make(map[uint32]*storage_ptr.StoragePointer)
	v2s[key] = ptr
	added := false
	for i := range node.Keys {
		if node.ChildPtrs[i] == nil {
			break
		}
		if !added && key < node.Keys[i] {
			keys = append(keys, key)
			added = true
		}
		keys = append(keys, node.Keys[i])
		v2s[node.Keys[i]] = node.ChildPtrs[i]
	}
	if !added {
		keys = append(keys, key)
	}

	if len(keys) <= int(b.m) {
		// no need to create new leaf node
		for i := len(keys); i < int(b.m); i++ {
			keys = append(keys, 0)
		}

		updatedChildPtrs := make([]*storage_ptr.StoragePointer, 0)
		for _, k := range keys {
			updatedChildPtrs = append(updatedChildPtrs, v2s[k])
		}

		updatedLeafNode := &storage.BPTNode{
			M:          node.M,
			Keys:       keys,
			ChildPtrs:  updatedChildPtrs,
			IsLeafNode: node.IsLeafNode,
		}
		return b.store.Update(nodePtr, updatedLeafNode.Serialize())
	}

	panic("not implemented")
	return nil
}
