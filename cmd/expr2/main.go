package main

import (
	"fmt"
	"github.com/geraldywy/cz4031_proj1/cmd/common"
	"github.com/geraldywy/cz4031_proj1/pkg/bptree"
)

func main() {
	fmt.Println("Begin experiment 2 - build a B+ tree on the attribute \"numVotes\" by inserting the records sequentially")
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
	fmt.Println("Reporting stats...")
	fmt.Println("========================================================================")
	fmt.Printf("Parameter n of the B+ tree: %d\n", bpt.M())
	fmt.Printf("Number of nodes of B+ tree: %d\n", bpt.NodeCnt())
	fmt.Printf("Levels in B+ tree: %d\n", bpt.Height())
	fmt.Printf("Content of the root node (only the keys): %v\n", bpt.RootNodeContent())

	fmt.Println("==================== Notes ====================")
	fmt.Println("Note: The number of nodes reported here only refers to B+ Tree Index nodes, not overflow nodes.")
	fmt.Println("Experiment 2 Done.")
}
