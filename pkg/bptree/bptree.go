package bptree

import (
	"errors"
	"github.com/geraldywy/cz4031_proj1/pkg/record"
	"github.com/geraldywy/cz4031_proj1/pkg/storage"
	"github.com/geraldywy/cz4031_proj1/pkg/storage_ptr"
	"math"
)

var (
	ErrKeyNotFound = errors.New("key not found in tree")
)

type BPTree interface {
	Insert(key uint32, ptr *storage_ptr.StoragePointer) error
	Search(key uint32) (record.Record, error)
	//// DeleteAll deletes all indexes with this key
	//DeleteAll(key uint32) error
	//SearchRange(from uint32, to uint32) ([]record.Record, error)
}

var _ BPTree = (*bptree)(nil)

type bptree struct {
	m           uint8
	store       storage.Storage
	rootNodePtr *storage_ptr.StoragePointer
}

func NewBPTree(m uint8, store storage.Storage) BPTree {
	return &bptree{m: m, store: store, rootNodePtr: nil}
}

// ptr here refers to the record pointer, not the indexedRecord ptr
func (b *bptree) Insert(key uint32, ptr *storage_ptr.StoragePointer) error {
	if b.rootNodePtr == nil {
		return b.startNewTree(key, ptr)
	}

	leaf, leafPtr, err := b.searchLeaf(key)
	if err != nil {
		return err
	}

	idx := -1
	for i, v := range leaf.Keys {
		if key == v && leaf.ChildPtrs[i] != nil {
			idx = i
			break
		}
	}

	// support duplicate keys
	if idx != -1 {
		lastPtr := leaf.ChildPtrs[idx]
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
		return b.store.Update(lastPtr, idxedRec)
	}

	if leaf.NumKeys < b.m {
		return b.insertIntoLeaf(leaf, leafPtr, key, ptr)
	}

	return b.insertIntoLeafAfterSplitting(leaf, leafPtr, key, ptr)
}

func (b *bptree) Search(key uint32) (record.Record, error) {
	leafNode, _, err := b.searchLeaf(key)
	if err != nil {
		return nil, err
	}
	var idx int
	for idx < int(leafNode.NumKeys) {
		if leafNode.Keys[idx] == key {
			break
		}
	}

	if idx == int(leafNode.NumKeys) {
		return nil, ErrKeyNotFound
	}

	buf, err := b.store.Read(leafNode.ChildPtrs[idx])
	if err != nil {
		return nil, err
	}
	return record.NewRecordFromBytes(buf), nil
}

func (b *bptree) searchLeaf(key uint32) (*storage.BPTNode, *storage_ptr.StoragePointer, error) {
	rootNodeBuf, err := b.store.Read(b.rootNodePtr)
	if err != nil {
		return nil, nil, err
	}
	rootNode := storage.NewBPTNodeFromBytes(rootNodeBuf)
	if rootNode == nil {
		return nil, nil, ErrKeyNotFound
	}
	tmp := rootNode
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

func (b *bptree) startNewTree(key uint32, ptr *storage_ptr.StoragePointer) error {
	idxedRecord := &storage.IndexedRecord{
		RecordPtr: ptr,
		NxtPtr:    nil,
	}
	idxPtr, err := b.store.Insert(idxedRecord)
	if err != nil {
		return err
	}

	root := storage.NewBPTNode(b.m, true)
	root.Keys[0] = key
	root.ChildPtrs[0] = idxPtr
	root.Parent = nil
	root.NumKeys = 1
	rootPtr, err := b.store.Insert(root)
	if err != nil {
		return err
	}
	b.rootNodePtr = rootPtr

	return nil
}

func (b *bptree) insertIntoLeaf(leaf *storage.BPTNode, leafPtr *storage_ptr.StoragePointer, key uint32, ptr *storage_ptr.StoragePointer) error {
	var insertionIdx int
	for insertionIdx < int(leaf.NumKeys) && key < leaf.Keys[insertionIdx] {
		insertionIdx++
	}

	for i := int(leaf.NumKeys); i > insertionIdx; i-- {
		leaf.Keys[i] = leaf.Keys[i-1]
		leaf.ChildPtrs[i] = leaf.ChildPtrs[i-1]
	}
	idxedRecord := &storage.IndexedRecord{
		RecordPtr: ptr,
		NxtPtr:    nil,
	}
	idxPtr, err := b.store.Insert(idxedRecord)
	if err != nil {
		return err
	}
	leaf.Keys[insertionIdx] = key
	leaf.ChildPtrs[insertionIdx] = idxPtr
	leaf.NumKeys++

	return b.store.Update(leafPtr, leaf)
}

func (b *bptree) insertIntoLeafAfterSplitting(leaf *storage.BPTNode, leafPtr *storage_ptr.StoragePointer, key uint32, ptr *storage_ptr.StoragePointer) error {
	newLeaf := storage.NewBPTNode(b.m, true)
	n := int(b.m + 1)
	tmpKeys := make([]uint32, n)
	tmpPtrs := make([]*storage_ptr.StoragePointer, n)
	var insertionIdx int
	for insertionIdx < len(leaf.Keys) && leaf.Keys[insertionIdx] < key {
		insertionIdx++
	}

	var j int
	for i := range leaf.Keys {
		if j == insertionIdx {
			j++
		}
		tmpKeys[j] = leaf.Keys[i]
		tmpPtrs[j] = leaf.ChildPtrs[i]
		j++
	}

	tmpKeys[insertionIdx] = key
	idxedRecord := &storage.IndexedRecord{
		RecordPtr: ptr,
		NxtPtr:    nil,
	}
	idxPtr, err := b.store.Insert(idxedRecord)
	if err != nil {
		return err
	}
	tmpPtrs[insertionIdx] = idxPtr

	leaf.NumKeys = 0
	x := n/2 + n%2
	for i := 0; i < x; i++ {
		leaf.Keys[i] = tmpKeys[i]
		leaf.ChildPtrs[i] = tmpPtrs[i]
		leaf.NumKeys++
	}
	j = 0
	for i := x; i < n; i++ {
		newLeaf.Keys[j] = tmpKeys[i]
		newLeaf.ChildPtrs[j] = tmpPtrs[i]
		newLeaf.NumKeys++
		j++
	}
	for i := leaf.NumKeys; i < b.m; i++ {
		leaf.ChildPtrs[i] = nil
	}
	// maintain leaf node's next pointers
	newLeaf.ChildPtrs[n-1] = leaf.ChildPtrs[n-1]
	newLeaf.Parent = leaf.Parent
	newLeafPtr, err := b.store.Insert(newLeaf)
	if err != nil {
		return err
	}
	leaf.ChildPtrs[n-1] = newLeafPtr
	if err := b.store.Update(leafPtr, leaf); err != nil {
		return err
	}

	return b.insertIntoParent(leaf, leafPtr, newLeaf.Keys[0], newLeaf, newLeafPtr)
}

func (b *bptree) insertIntoParent(left *storage.BPTNode, leftPtr *storage_ptr.StoragePointer, key uint32, right *storage.BPTNode, rightPtr *storage_ptr.StoragePointer) error {
	parentPtr := left.Parent
	if parentPtr == nil {
		return b.insertIntoNewRoot(left, leftPtr, key, right, rightPtr)
	}

	buf, err := b.store.Read(parentPtr)
	if err != nil {
		return err
	}
	parent := storage.NewBPTNodeFromBytes(buf)
	var leftIdx int
	for leftIdx <= int(parent.NumKeys) && parent.ChildPtrs[leftIdx] != leftPtr {
		leftIdx++
	}

	if parent.NumKeys < b.m {
		return b.insertIntoNode(parent, parentPtr, leftIdx, key, rightPtr)
	}

	return b.insertIntoNodeAfterSplitting(parent, parentPtr, leftIdx, key, rightPtr)
}

func (b *bptree) insertIntoNewRoot(left *storage.BPTNode, leftPtr *storage_ptr.StoragePointer, key uint32, right *storage.BPTNode, rightPtr *storage_ptr.StoragePointer) error {
	newRoot := storage.NewBPTNode(b.m, false)
	newRoot.Keys[0] = key
	newRoot.ChildPtrs[0] = leftPtr
	newRoot.ChildPtrs[1] = rightPtr
	newRoot.NumKeys = 1
	newRootPtr, err := b.store.Insert(newRoot)
	if err != nil {
		return err
	}
	b.rootNodePtr = newRootPtr
	left.Parent = newRootPtr
	if err := b.store.Update(leftPtr, left); err != nil {
		return err
	}
	right.Parent = newRootPtr
	return b.store.Update(rightPtr, right)
}

func (b *bptree) insertIntoNode(node *storage.BPTNode, nodePtr *storage_ptr.StoragePointer, leftIdx int, key uint32, rightPtr *storage_ptr.StoragePointer) error {
	for i := int(node.NumKeys); i > leftIdx; i-- {
		node.Keys[i] = node.Keys[i-1]
		node.ChildPtrs[i+1] = node.ChildPtrs[i]
	}
	node.Keys[leftIdx] = key
	node.ChildPtrs[leftIdx+1] = rightPtr
	node.NumKeys++

	return b.store.Update(nodePtr, node)
}

func (b *bptree) insertIntoNodeAfterSplitting(oldNode *storage.BPTNode, oldNodePtr *storage_ptr.StoragePointer, leftIdx int,
	key uint32, rightPtr *storage_ptr.StoragePointer) error {
	n := int(b.m) + 1
	tmpKeys := make([]uint32, n)
	var j int
	for i := 0; i < int(oldNode.NumKeys); i++ {
		if j == leftIdx {
			j++
		}
		tmpKeys[j] = oldNode.Keys[i]
		j++
	}

	tmpPtrs := make([]*storage_ptr.StoragePointer, n+1)
	j = 0
	for i := 0; i < int(oldNode.NumKeys)+1; i++ {
		if j == leftIdx+1 {
			j++
		}
		tmpPtrs[j] = oldNode.ChildPtrs[i]
		j++
	}

	tmpKeys[leftIdx] = key
	tmpPtrs[leftIdx+1] = rightPtr

	x := n/2 + n%2
	oldNode.NumKeys = 0
	for i := 0; i < x-1; i++ {
		oldNode.Keys[i] = tmpKeys[i]
		oldNode.ChildPtrs[i] = tmpPtrs[i]
		oldNode.NumKeys++
	}
	oldNode.ChildPtrs[x-1] = tmpPtrs[x-1]
	if err := b.store.Update(oldNodePtr, oldNode); err != nil {
		return err
	}

	kPrime := tmpKeys[x-1]
	j = 0
	newNode := storage.NewBPTNode(b.m, false)
	for i := x; i < n; i++ {
		newNode.ChildPtrs[j] = tmpPtrs[i]
		newNode.Keys[j] = tmpKeys[i]
		newNode.NumKeys++
		j++
	}
	newNode.ChildPtrs[newNode.NumKeys] = tmpPtrs[n]
	newNode.Parent = oldNode.Parent
	newNodePtr, err := b.store.Insert(newNode)
	if err != nil {
		return err
	}
	for i := 0; i <= int(newNode.NumKeys); i++ {
		buf, err := b.store.Read(newNode.ChildPtrs[i])
		if err != nil {
			return err
		}
		child := storage.NewBPTNodeFromBytes(buf)
		child.Parent = newNodePtr
		if err := b.store.Update(newNode.ChildPtrs[i], child); err != nil {
			return err
		}
	}

	return b.insertIntoParent(oldNode, oldNodePtr, kPrime, newNode, newNodePtr)
}
