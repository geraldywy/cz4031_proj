package bptree

import (
	"errors"
	"github.com/geraldywy/cz4031_proj1/pkg/record"
	"github.com/geraldywy/cz4031_proj1/pkg/storage"
	"github.com/geraldywy/cz4031_proj1/pkg/storage_ptr"
	"github.com/geraldywy/cz4031_proj1/pkg/utils"
	"reflect"
)

var (
	ErrKeyNotFound          = errors.New("key not found in tree")
	ErrNeighbourIdxNotFound = errors.New("neighbour idx not found")
)

type BPTree interface {
	Insert(key uint32, ptr *storage_ptr.StoragePointer) error
	Search(key uint32) ([]record.Record, []*storage_ptr.StoragePointer, error)
	SearchRange(from uint32, to uint32) ([]record.Record, error)
	// DeleteAll deletes all indexes with this key
	DeleteAll(key uint32) error
}

var _ BPTree = (*bptree)(nil)

type bptree struct {
	m           uint8
	store       storage.Storage
	rootNodePtr *storage_ptr.StoragePointer
}

func NewBPTree(m uint8, store storage.Storage) BPTree {
	storage.M = m
	return &bptree{m: m, store: store, rootNodePtr: nil}
}

func (b *bptree) DeleteAll(key uint32) error {
	_, recordPtrs, err := b.Search(key)
	if err != nil {
		return err
	}
	// remove in storage all indexed records under this key
	for _, ptr := range recordPtrs {
		if err := b.store.Delete(ptr); err != nil {
			return err
		}
	}

	leafNode, leafNodePtr, err := b.searchLeaf(key)
	if err != nil {
		return err
	}

	return b.deleteEntry(leafNode, leafNodePtr, key, recordPtrs[0])
}

func (b *bptree) deleteEntry(node *storage.BPTNode, nodePtr *storage_ptr.StoragePointer, key uint32, indexedRecPtr *storage_ptr.StoragePointer) error {
	var err error
	node, err = b.removeEntryFromNode(node, key, indexedRecPtr)
	if err != nil {
		return err
	}

	if nodePtr == b.rootNodePtr {
		if err := b.store.Update(nodePtr, node); err != nil {
			return err
		}
		return b.adjustRoot(node)
	}
	n := int(b.m)
	minKeys := (n+1)/2 + (n+1)%2 - 1
	if node.IsLeafNode {
		minKeys = n/2 + n%2
	}

	if int(node.NumKeys) >= minKeys {
		return b.store.Update(nodePtr, node)
	}

	parentBuf, err := b.store.Read(node.Parent)
	if err != nil {
		return err
	}
	parentNode := storage.NewBPTNodeFromBytes(parentBuf)
	neighIdx, err := b.getNeighbourIdx(parentNode, nodePtr)
	if err != nil {
		return ErrNeighbourIdxNotFound
	}
	kPrimeIdx := utils.Max(0, neighIdx)
	kPrime := parentNode.Keys[kPrimeIdx]

	x := utils.Max(1, neighIdx)
	neighPtr := parentNode.ChildPtrs[x]
	neighBuf, err := b.store.Read(neighPtr)
	if err != nil {
		return err
	}
	neighNode := storage.NewBPTNodeFromBytes(neighBuf)
	capacity := int(b.m)
	if node.IsLeafNode {
		capacity++
	}

	if int(neighNode.NumKeys+node.NumKeys) < capacity {
		return b.coalesceNodes(node, nodePtr, neighNode, neighPtr, neighIdx, kPrime)
	}

	return b.redistributeNodes(node, nodePtr, neighNode, neighPtr, neighIdx, kPrimeIdx, kPrime)
}

func (b *bptree) redistributeNodes(node *storage.BPTNode, nodePtr *storage_ptr.StoragePointer, neighNode *storage.BPTNode,
	neighPtr *storage_ptr.StoragePointer, neighIdx int, kPrimeIdx int, kPrime uint32) error {
	parentNodePtr := node.Parent
	parentBuf, err := b.store.Read(parentNodePtr)
	if err != nil {
		return err
	}
	parentNode := storage.NewBPTNodeFromBytes(parentBuf)

	if neighIdx != -1 {
		if !node.IsLeafNode {
			node.ChildPtrs[node.NumKeys+1] = node.ChildPtrs[node.NumKeys]
		}
		for i := node.NumKeys; i > 0; i-- {
			node.Keys[i] = node.Keys[i-1]
			node.ChildPtrs[i] = node.ChildPtrs[i-1]
		}
		if !node.IsLeafNode {
			node.ChildPtrs[0] = neighNode.ChildPtrs[neighNode.NumKeys]
			tmpPtr := node.ChildPtrs[0]
			tmpBuf, err := b.store.Read(tmpPtr)
			if err != nil {
				return err
			}
			tmpNode := storage.NewBPTNodeFromBytes(tmpBuf)
			tmpNode.Parent = nodePtr
			if err := b.store.Update(tmpPtr, tmpNode); err != nil {
				return err
			}
			neighNode.ChildPtrs[neighNode.NumKeys] = nil
			node.Keys[0] = kPrime
			parentNode.Keys[kPrimeIdx] = node.Keys[0]
		} else {
			node.ChildPtrs[0] = neighNode.ChildPtrs[neighNode.NumKeys-1]
			neighNode.ChildPtrs[neighNode.NumKeys-1] = nil
			node.Keys[0] = neighNode.Keys[neighNode.NumKeys-1]
			parentNode.Keys[kPrimeIdx] = node.Keys[0]
		}
	} else {
		if node.IsLeafNode {
			node.Keys[node.NumKeys] = neighNode.Keys[0]
			node.ChildPtrs[node.NumKeys] = neighNode.ChildPtrs[0]
			parentNode.Keys[kPrimeIdx] = neighNode.Keys[1]
		} else {
			node.Keys[node.NumKeys] = kPrime
			node.ChildPtrs[int(node.NumKeys)+1] = neighNode.ChildPtrs[0]
			tmpPtr := node.ChildPtrs[int(node.NumKeys)+1]
			tmpBuf, err := b.store.Read(tmpPtr)
			if err != nil {
				return err
			}
			tmpNode := storage.NewBPTNodeFromBytes(tmpBuf)
			tmpNode.Parent = nodePtr
			if err := b.store.Update(tmpPtr, tmpNode); err != nil {
				return err
			}
			parentNode.Keys[kPrime] = neighNode.Keys[0]
		}
		i := 0
		for i = 0; i < int(neighNode.NumKeys)-1; i++ {
			neighNode.Keys[i] = neighNode.Keys[i+1]
			neighNode.ChildPtrs[i] = neighNode.ChildPtrs[i+1]
		}
		if !node.IsLeafNode {
			neighNode.ChildPtrs[i] = neighNode.ChildPtrs[i+1]
		}
	}
	node.NumKeys++
	neighNode.NumKeys--
	if err := b.store.Update(nodePtr, node); err != nil {
		return err
	}
	if err := b.store.Update(neighPtr, neighNode); err != nil {
		return err
	}
	if err := b.store.Update(parentNodePtr, parentNode); err != nil {
		return err
	}

	return nil
}

func (b *bptree) coalesceNodes(node *storage.BPTNode, nodePtr *storage_ptr.StoragePointer,
	neighNode *storage.BPTNode, neighPtr *storage_ptr.StoragePointer, neighIdx int, kPrime uint32) error {
	if neighIdx == -1 {
		node, neighNode = neighNode, node
		nodePtr, neighPtr = neighPtr, nodePtr
	}

	neighInsertionIdx := neighNode.NumKeys
	if node.IsLeafNode {
		i := neighInsertionIdx
		for j := 0; j < int(neighNode.NumKeys); j++ {
			neighNode.Keys[i] = node.Keys[j]
			neighNode.ChildPtrs[i] = node.ChildPtrs[j]
			neighNode.NumKeys++
			i++
		}
		neighNode.ChildPtrs[int(b.m)] = node.ChildPtrs[int(b.m)]
	} else {
		neighNode.Keys[neighInsertionIdx] = kPrime
		neighNode.NumKeys++
		j := 0
		i := neighInsertionIdx + 1
		for j < int(node.NumKeys) {
			neighNode.Keys[i] = node.Keys[j]
			neighNode.ChildPtrs[i] = node.ChildPtrs[j]
			neighNode.NumKeys++
			i++
			j++
		}
		neighNode.ChildPtrs[i] = node.ChildPtrs[j]
		for i := 0; i <= int(neighNode.NumKeys); i++ {
			childPtr := neighNode.ChildPtrs[i]
			buf, err := b.store.Read(childPtr)
			if err != nil {
				return err
			}
			childNode := storage.NewBPTNodeFromBytes(buf)
			childNode.Parent = neighPtr
			if err := b.store.Update(childPtr, childNode); err != nil {
				return err
			}
		}
	}

	if err := b.store.Update(neighPtr, neighNode); err != nil {
		return err
	}

	if err := b.store.Delete(nodePtr); err != nil {
		return err
	}

	parentNodePtr := node.Parent
	parentBuf, err := b.store.Read(parentNodePtr)
	if err != nil {
		return err
	}
	parentNode := storage.NewBPTNodeFromBytes(parentBuf)
	return b.deleteEntry(parentNode, parentNodePtr, kPrime, nodePtr)
}

func (b *bptree) getNeighbourIdx(parentNode *storage.BPTNode, nodePtr *storage_ptr.StoragePointer) (int, error) {
	var i int

	for i = 0; i <= int(parentNode.NumKeys); i++ {
		if reflect.DeepEqual(parentNode.ChildPtrs[i], nodePtr) {
			return i - 1, nil
		}
	}

	return 0, ErrNeighbourIdxNotFound
}

func (b *bptree) adjustRoot(rootNode *storage.BPTNode) error {
	if rootNode.NumKeys > 0 {
		return nil
	}
	if err := b.store.Delete(b.rootNodePtr); err != nil {
		return err
	}

	var newRootPtr *storage_ptr.StoragePointer
	if !rootNode.IsLeafNode {
		newRootPtr = rootNode.ChildPtrs[0]
		newRootBuf, err := b.store.Read(newRootPtr)
		if err != nil {
			return err
		}
		newRoot := storage.NewBPTNodeFromBytes(newRootBuf)
		newRoot.Parent = nil
		b.store.Update(newRootPtr, newRoot)
	} else {
		newRootPtr = nil
	}
	b.rootNodePtr = newRootPtr

	return nil
}

func (b *bptree) removeEntryFromNode(node *storage.BPTNode, key uint32, ptr *storage_ptr.StoragePointer) (*storage.BPTNode, error) {
	var i int
	for i < int(node.NumKeys) && node.Keys[i] != key {
		i += 1
	}

	for j := i + 1; j < int(node.NumKeys); j++ {
		node.Keys[j-1] = node.Keys[j]
	}
	ptrCnt := int(node.NumKeys)
	if !node.IsLeafNode {
		ptrCnt++
	}

	i = 0
	for !reflect.DeepEqual(node.ChildPtrs[i], ptr) {
		i++
	}

	for j := i + 1; j < ptrCnt; j++ {
		node.ChildPtrs[j-1] = node.ChildPtrs[j]
	}
	node.NumKeys--

	start := int(node.NumKeys)
	end := int(b.m)
	if !node.IsLeafNode {
		start++
		end++
	}
	for start < end {
		node.ChildPtrs[start] = nil
		start++
	}

	return node, nil
}

func (b *bptree) SearchRange(from uint32, to uint32) ([]record.Record, error) {
	leafNode, _, err := b.searchLeaf(from)
	if err != nil {
		return nil, err
	}

	res := make([]record.Record, 0)
	idx := 0
	for leafNode != nil {
		v := leafNode.Keys[idx]
		if v > to {
			break
		} else if v >= from {
			buf, err := b.store.Read(leafNode.ChildPtrs[idx])
			if err != nil {
				return nil, err
			}
			idxedRec := storage.IndexedRecordFromBytes(buf)
			for idxedRec != nil {
				buf, err := b.store.Read(idxedRec.RecordPtr)
				if err != nil {
					return nil, err
				}
				res = append(res, record.NewRecordFromBytes(buf))
				buf, err = b.store.Read(idxedRec.NxtPtr)
				if err != nil {
					return nil, err
				}
				idxedRec = storage.IndexedRecordFromBytes(buf)
			}
		}
		idx++
		if idx == int(leafNode.NumKeys) {
			buf, err := b.store.Read(leafNode.ChildPtrs[idx])
			if err != nil {
				return nil, err
			}
			leafNode = storage.NewBPTNodeFromBytes(buf)
			idx = 0
		}
	}

	return res, nil
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
	for i := 0; i < int(leaf.NumKeys); i++ {
		if key == leaf.Keys[i] {
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

func (b *bptree) Search(key uint32) ([]record.Record, []*storage_ptr.StoragePointer, error) {
	leafNode, _, err := b.searchLeaf(key)
	if err != nil {
		return nil, nil, err
	}
	var idx int
	for idx < int(leafNode.NumKeys) {
		if leafNode.Keys[idx] == key {
			break
		}
		idx++
	}
	if idx == int(leafNode.NumKeys) {
		return nil, nil, ErrKeyNotFound
	}
	res := make([]record.Record, 0)
	resPtrs := make([]*storage_ptr.StoragePointer, 0)
	resPtrs = append(resPtrs, leafNode.ChildPtrs[idx])
	buf, err := b.store.Read(leafNode.ChildPtrs[idx])
	if err != nil {
		return nil, nil, err
	}
	idxedRec := storage.IndexedRecordFromBytes(buf)
	for idxedRec != nil {
		buf, err := b.store.Read(idxedRec.RecordPtr)
		if err != nil {
			return nil, nil, err
		}
		res = append(res, record.NewRecordFromBytes(buf))
		resPtrs = append(resPtrs, idxedRec.NxtPtr)
		buf, err = b.store.Read(idxedRec.NxtPtr)
		if err != nil {
			return nil, nil, err
		}
		idxedRec = storage.IndexedRecordFromBytes(buf)
	}
	resPtrs = resPtrs[:len(resPtrs)-1] // last ptr is nil
	return res, resPtrs, nil
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
		// TODO: maybe change linear to binary search
		idx := 0
		for idx < int(tmp.NumKeys) {
			if key >= tmp.Keys[idx] {
				idx++
			} else {
				break
			}
		}
		tmpPtr = tmp.ChildPtrs[idx]
		buf, err := b.store.Read(tmpPtr)
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
	for insertionIdx < int(leaf.NumKeys) && key > leaf.Keys[insertionIdx] {
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
	x := (n-1)/2 + (n-1)%2
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
	for leftIdx <= int(parent.NumKeys) && !parent.ChildPtrs[leftIdx].DeepEqual(leftPtr) {
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
