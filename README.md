# CZ4031 Project 1

## Prerequisites
1. Go version 1.19^

## Run locally
There are 5 experiments, each contained within their own script, located in the cmd folder.
The output is written to stdout. 

In general, the commands are ran from project root roughly as follows:
`go run ./cmd/expr<no>/main.go`

Below are the 5 commands to run each individual experiment, included below each are also the output I got on my local machine.

### Experiment 1
```shell
go run ./cmd/expr1/main.go
```
#### Output
```
$ go run ./cmd/expr1/main.go
Begin experiment 1 - insertion of records into disk
Reporting stats...
========================================================================
Total number of records: 1070318
Record size: 19
Average number of record stored per block: 9.97
Total number of blocks: 107314
==================== Notes ====================
Note: Because records are stored across blocks, instead of calculating the number of records stored in a block, we calculate the avg number instead.
Experiment 1 Done.
```

### Experiment 2
```shell
go run ./cmd/expr2/main.go
```
#### Output
```
$ go run ./cmd/expr2/main.go
Begin experiment 2 - build a B+ tree on the attribute "numVotes" by inserting the records sequentially
Reporting stats...
========================================================================
Parameter n of the B+ tree: 3
Number of nodes of B+ tree: 11636
Levels in B+ tree: 9
Content of the root node (only the keys): [3430 9113 24759]
==================== Notes ====================
Note: The number of nodes reported here only refers to B+ Tree Index nodes, not overflow nodes.
Experiment 2 Done.

```

### Experiment 3
```shell
go run ./cmd/expr3/main.go
```
#### Output
```
$ go run ./cmd/expr3/main.go
Begin experiment 3 - retrieve those movies with the “numVotes” equal to 500
Reporting stats...
========================================================================
Number of index nodes the process accesses: 9
Number of data blocks the process accesses: 243
Average of “averageRating’s” of the records that are returned: 6.732
Running time of the retrieval process: 79 (microseconds) 
========================================================================
Start brute force linear scan method.
Number of data blocks brute force linear scan method accesses: 1167127
Average of “averageRating’s” of the records that are returned using brute force method: 6.732
Running time of the brute force retrieval process: 321694 (microseconds) 
==================== Notes ====================
Note: The number of index nodes accounted for only consist of the index nodes in the B+ tree, not the overflow blocks.
Also, the number of data blocks accounted for could potentially be counted more than once, since records are spanned. We do not count only unique blocks since no block cache mechanism are intended.
Experiment 3 Done.
```

### Experiment 4
```shell
go run ./cmd/expr4/main.go
```
#### Output
```
$ go run ./cmd/expr4/main.go
Begin experiment 4 - retrieve those movies with the attribute “numVotes” from 30,000 to 40,000, both inclusively
Reporting stats...
========================================================================
Number of index nodes the process accesses: 401
Number of data blocks the process accesses: 2534
Average of “averageRating’s” of the records that are returned: 6.728
Running time of the retrieval process: 1196 (microseconds) 
========================================================================
Start brute force linear scan method.
Number of data blocks brute force linear scan method accesses: 1167127
Average of “averageRating’s” of the records that are returned using brute force method: 6.728
Running time of the brute force retrieval process: 335716 (microseconds) 
==================== Notes ====================
Note: The number of index nodes accounted for only consist of the index nodes in the B+ tree, not the overflow blocks.
Also, the number of data blocks accounted for could potentially be counted more than once, since records are spanned. We do not count only unique blocks since no block cache mechanism are intended.
Experiment 4 Done.
```

### Experiment 5
```shell
go run ./cmd/expr5/main.go
```
#### Output
```
$ go run ./cmd/expr5/main.go
Begin experiment 5 - delete those movies with the attribute “numVotes” equal to 1,000, update the B+ tree accordingly
Reporting stats *BEFORE* deletion, the original B+ Tree stats:
========================================================================
The number nodes of the updated B+ tree: 11714
The number of levels of the updated B+ tree: 9
The content of the root node of the updated B+ tree(only the keys): [6423 15808 31598]
========================================================================
Reporting stats *AFTER* deletion, the updated B+ Tree stats:
========================================================================
The number nodes of the updated B+ tree: 11714
The number of levels of the updated B+ tree: 9
The content of the root node of the updated B+ tree(only the keys): [6423 15808 31598]
Running time of the deletion process: 73 (microseconds) 
(Extra) The number of records deleted: 42
========================================================================
Start brute force linear deletion method.
Number of data blocks brute force linear deletion method accesses: 1167127
Running time of the brute force deletion process: 326236 (microseconds) 
(Extra) The number of records brute force method deleted: 42
==================== Notes ====================
Note: The number of index nodes accounted for only consist of the index nodes in the B+ tree, not the overflow blocks.
Also, the number of data blocks accounted for could potentially be counted more than once, since records are spanned. We do not count only unique blocks since no block cache mechanism are intended.
Experiment 5 Done.

```
