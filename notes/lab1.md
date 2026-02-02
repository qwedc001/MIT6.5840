# Lab 1: MapReduce

对于大规模数据集使用的编程模型。

`map({original_key,original_value}) -> {intermediate_key,intermediate_value}`
`reduce({intermediate_key,intermediate_value}) -> {result_key,result_value}`

例如（Lab Task Related）：

`map({filename,content}) -> {word_from_content,"1"}`
`reduce({word_from_content,"1"}) -> {word,word_count}`

这样做的逻辑是为了能将大规模数据集分成多份后，使用 program 进行多个 worker assign 来分布式完成任务。在 assign worker 的时候，相同 intermediate_key 的数据会被发送到同一个 worker。

```bash
qwedc001@qwedc001-archlinux ~/6.5840/src > make mr
go build -race -o main/mrsequential main/mrsequential.go
go build -race -o main/mrcoordinator main/mrcoordinator.go
go build -race -o main/mrworker main/mrworker.go&
(cd mrapps && go build -race -buildmode=plugin wc.go) || exit 1
(cd mrapps && go build -race -buildmode=plugin indexer.go) || exit 1
(cd mrapps && go build -race -buildmode=plugin mtiming.go) || exit 1
(cd mrapps && go build -race -buildmode=plugin rtiming.go) || exit 1
(cd mrapps && go build -race -buildmode=plugin jobcount.go) || exit 1
(cd mrapps && go build -race -buildmode=plugin early_exit.go) || exit 1
(cd mrapps && go build -race -buildmode=plugin crash.go) || exit 1
(cd mrapps && go build -race -buildmode=plugin nocrash.go) || exit 1
cd mr; go test -v -race 
=== RUN   TestWc
Worker 129341: Exiting
Worker 129339: Exiting
Worker 129340: Exiting
--- PASS: TestWc (17.24s)
=== RUN   TestIndexer
Worker 129588: Exiting
Worker 129586: Exiting
--- PASS: TestIndexer (8.83s)
=== RUN   TestMapParallel
Worker 129679: Exiting
Worker 129678: Exiting
--- PASS: TestMapParallel (8.05s)
=== RUN   TestReduceParallel
Worker 129766: Exiting
Worker 129765: Exiting
--- PASS: TestReduceParallel (9.04s)
=== RUN   TestJobCount
Worker 129855: Exiting
Worker 129853: Exiting
Worker 129854: Exiting
Worker 129856: Exiting
--- PASS: TestJobCount (11.04s)
=== RUN   TestEarlyExit
Worker 129991: Exiting
Worker 129992: Exiting
Worker 129990: Exiting
Worker 129988: Exiting
--- PASS: TestEarlyExit (7.04s)
=== RUN   TestCrashWorker
Worker 130230: Exiting
Worker 130186: Exiting
Worker 130285: Exiting
Worker 130332: Exiting
Worker 130350: Exiting
Worker 130357: Exiting
--- PASS: TestCrashWorker (43.21s)
PASS
ok      6.5840/mr       105.484s
```