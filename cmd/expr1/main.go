package main

import (
	"encoding/csv"
	"fmt"
	"github.com/geraldywy/cz4031_proj1/pkg/consts"
	"github.com/geraldywy/cz4031_proj1/pkg/record"
	"github.com/geraldywy/cz4031_proj1/pkg/storage"
	"github.com/geraldywy/cz4031_proj1/pkg/storage_ptr"
	"io"
	"os"
	"strconv"
)

func main() {
	fmt.Println("Begin experiment one - insertion of records into disk")
	store, _ := LoadTsvIntoStore("./data.tsv")
	fmt.Println("Reporting stats...")
	fmt.Printf("Total number of records: %d\n",store.RecordCnt())
	fmt.Printf("Record size: %d\n", consts.RecordSize)
	fmt.Printf("Average number of record stored per block: %.2f\n", store.AvgRecordPerBlock())
	fmt.Printf("Total number of blocks: %d\n", store.BlockCnt())

	fmt.Println("Done.")
	fmt.Println("Note: Because records are stored across blocks, instead of calculating the number of records stored in a block, we calculate the avg number instead.")
}

func LoadTsvIntoStore(filename string) (storage.Storage, map[int32][]*storage_ptr.StoragePointer) {
	f, err := os.Open(filename)
	if err != nil {
		panic(err)
	}

	defer f.Close()

	csvReader := csv.NewReader(f)
	csvReader.Comma = '\t'
	store := storage.NewStorage(consts.DiskCapacity, consts.BlockSize)
	res := make(map[int32][]*storage_ptr.StoragePointer)
	csvReader.Read()
	for {
		row, err := csvReader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			panic(err)
		}

		avgRating, err := strconv.ParseFloat(row[1], 32)
		if err != nil {
			panic(err)
		}
		numVotes, err := strconv.Atoi(row[2])
		if err != nil {
			panic(err)
		}
		nv := int32(numVotes)
		newRec := record.NewRecord(row[0], float32(avgRating), nv)
		ptr, err := store.Insert(newRec)
		if err != nil {
			panic(err)
		}
		res[nv] = append(res[nv], ptr)
	}

	return store, res
}
