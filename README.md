# mapreduce

A distributed map reduce implementation based on Lab 1 from MIT's 6.5840 Distributed Systems course (<https://pdos.csail.mit.edu/6.824/index.html>)

To build

```bash
go build -buildmode=plugin mrapps/wc.go
```

To run a simple sequential map reduce

```bash
go run ./cmd/mrsequential/ wc.so pg-*.txt
more mr-out-0
```