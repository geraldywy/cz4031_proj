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
	fmt.Println("Begin experiment 4 - retrieve those movies with the attribute “numVotes” from 30,000 to 40,000, both inclusively")
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

	const from = 30000
	const to = 40000
	start := time.Now()
	records, nodeAccessedCnt, dataBlockCnt, err := bpt.SearchRange(from, to)
	if err != nil {
		panic(err)
	}
	elapsedTime :=  time.Since(start).Microseconds()


	avgAvgRating := float64(0)
	for _, rec := range records {
		avgAvgRating += float64(rec.AvgRating())
	}
	avgAvgRating /= float64(len(records))
	fmt.Println("Reporting stats...")
	fmt.Println("========================================================================")
	fmt.Printf("Number of index nodes the process accesses: %d\n", nodeAccessedCnt)
	fmt.Printf("Number of data blocks the process accesses: %d\n", dataBlockCnt)
	fmt.Printf("Average of “averageRating’s” of the records that are returned: %.3f\n", avgAvgRating)
	fmt.Printf("Running time of the retrieval process: %v (microseconds) \n", elapsedTime)
	fmt.Println("========================================================================")
	fmt.Println("Start brute force linear scan method.")
	bruteForceStart := time.Now()
	bruteForceBlkCnt, bruteForceAvg := bruteForceRangeRead(from, to, store, mapping)
	bruteForceElapsedTime := time.Since(bruteForceStart).Microseconds()
	fmt.Printf("Number of data blocks brute force linear scan method accesses: %d\n", bruteForceBlkCnt)
	fmt.Printf("Average of “averageRating’s” of the records that are returned using brute force method: %.3f\n", bruteForceAvg)
	fmt.Printf("Running time of the brute force retrieval process: %v (microseconds) \n", bruteForceElapsedTime)
	fmt.Println("==================== Notes ====================")
	fmt.Println("Note: The number of index nodes accounted for only consist of the index nodes in the B+ tree, not the overflow blocks.")
	fmt.Println("Also, the number of data blocks accounted for could potentially be counted more than once, since records are spanned. We do not count only unique blocks since no block cache mechanism are intended.")
	fmt.Println("Experiment 4 Done.")
}

func bruteForceRangeRead(from int32, to int32, store storage.Storage, recordPtrs map[int32][]*storage_ptr.StoragePointer) (int, float64) {
	bCnt := 0
	var avg, ttl float64
	for _, ptrs := range recordPtrs {
		for _, ptr := range ptrs {
			recordBuf, cnt, err := store.Read(ptr)
			if err != nil {
				panic(err)
			}
			bCnt += cnt
			rec := record.NewRecordFromBytes(recordBuf)
			if rec.NumVotes() >= from && rec.NumVotes() <= to {
				avg += float64(rec.AvgRating())
				ttl++
			}
		}
	}

	return bCnt, avg/ttl
}
