package main

import (
	"fmt"
	"github.com/geraldywy/cz4031_proj1/cmd/common"
	"github.com/geraldywy/cz4031_proj1/pkg/consts"
)

func main() {
	fmt.Println("Begin experiment 1 - insertion of records into disk")
	store, _ := common.LoadTsvIntoStore("./data.tsv")
	fmt.Println("Reporting stats...")
	fmt.Println("========================================================================")
	fmt.Printf("Total number of records: %d\n",store.RecordCnt())
	fmt.Printf("Record size: %d\n", consts.RecordSize)
	fmt.Printf("Average number of record stored per block: %.2f\n", store.AvgRecordPerBlock())
	fmt.Printf("Total number of blocks: %d\n", store.BlockCnt())

	fmt.Println("==================== Notes ====================")
	fmt.Println("Note: Because records are stored across blocks, instead of calculating the number of records stored in a block, we calculate the avg number instead.")
	fmt.Println("Experiment 1 Done.")
}
