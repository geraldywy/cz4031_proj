package main

import (
	"fmt"
	"github.com/geraldywy/cz4031_proj1/cmd/common"
	"github.com/geraldywy/cz4031_proj1/pkg/bptree"
	"github.com/geraldywy/cz4031_proj1/pkg/record"
	"github.com/geraldywy/cz4031_proj1/pkg/storage"
	"github.com/geraldywy/cz4031_proj1/pkg/storage_ptr"
	"time"
)

func main() {
	fmt.Println("Begin experiment 5 - delete those movies with the attribute “numVotes” equal to 1,000, update the B+ tree accordingly")
	store, mapping := common.LoadTsvIntoStore("./data.tsv")
	const m = 3
	bpt := bptree.NewBPTree(m, store)
	for k, ptrs := range mapping {
		for _, ptr := range ptrs {
			if err := bpt.Insert(uint32(k), ptr); err != nil {
				panic(err)
			}
		}
	}

	fmt.Println("Reporting stats *BEFORE* deletion, the original B+ Tree stats:")
	fmt.Println("========================================================================")
	fmt.Printf("The number nodes of the updated B+ tree: %d\n", bpt.NodeCnt())
	fmt.Printf("The number of levels of the updated B+ tree: %d\n", bpt.Height())
	fmt.Printf("The content of the root node of the updated B+ tree(only the keys): %v\n", bpt.RootNodeContent())
	fmt.Println("========================================================================")

	const delKey = 1000
	start := time.Now()
	delRecordCnt, err := bpt.DeleteAll(delKey)
	if err != nil {
		panic(err)
	}
	elapsedTime :=  time.Since(start).Microseconds()
	fmt.Println("Reporting stats *AFTER* deletion, the updated B+ Tree stats:")
	fmt.Println("========================================================================")
	fmt.Printf("The number nodes of the updated B+ tree: %d\n", bpt.NodeCnt())
	fmt.Printf("The number of levels of the updated B+ tree: %d\n", bpt.Height())
	fmt.Printf("The content of the root node of the updated B+ tree(only the keys): %v\n", bpt.RootNodeContent())
	fmt.Printf("Running time of the deletion process: %v (microseconds) \n", elapsedTime)
	fmt.Printf("(Extra) The number of records deleted: %d\n", delRecordCnt)
	fmt.Println("========================================================================")

	fmt.Println("Start brute force linear deletion method.")
	refreshedStore, refreshedMapping := common.LoadTsvIntoStore("./data.tsv") // reinsert records
	bruteForceStart := time.Now()
	bruteForceBlkCnt, bruteForceDelCnt := bruteForceDeletion(delKey, refreshedStore, refreshedMapping)
	bruteForceElapsedTime := time.Since(bruteForceStart).Microseconds()
	fmt.Printf("Number of data blocks brute force linear deletion method accesses: %d\n", bruteForceBlkCnt)
	fmt.Printf("Running time of the brute force deletion process: %v (microseconds) \n", bruteForceElapsedTime)
	fmt.Printf("(Extra) The number of records brute force method deleted: %d\n", bruteForceDelCnt)

	fmt.Println("==================== Notes ====================")
	fmt.Println("Note: The number of index nodes accounted for only consist of the index nodes in the B+ tree, not the overflow blocks.")
	fmt.Println("Also, the number of data blocks accounted for could potentially be counted more than once, since records are spanned. We do not count only unique blocks since no block cache mechanism are intended.")
	fmt.Println("Experiment 5 Done.")
}

func bruteForceDeletion(delKey int32, store storage.Storage, recordPtrs map[int32][]*storage_ptr.StoragePointer) (int, int) {
	bCnt := 0
	delRecordCnt := 0
	for _, ptrs := range recordPtrs {
		for _, ptr := range ptrs {
			recordBuf, cnt, err := store.Read(ptr)
			if err != nil {
				panic(err)
			}
			bCnt += cnt
			rec := record.NewRecordFromBytes(recordBuf)
			if rec.NumVotes() == delKey {
				if err := store.Delete(ptr); err != nil {
					panic(err)
				}
				delRecordCnt++
			}
		}
	}

	return bCnt, delRecordCnt
}
