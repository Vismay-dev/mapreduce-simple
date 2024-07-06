## mapreduce-simple

![Go CI](https://github.com/Vismay-dev/mapreduce-simple/actions/workflows/go-format.yml/badge.svg)

Simple and minimal implementation of [the MapReduce distributed computation framework](https://static.googleusercontent.com/media/research.google.com/en//archive/mapreduce-osdi04.pdf) introduced by Jeffrey Dean and Sanjay Ghemawat @ Google Inc. in 2004.

MapReduce is a programming model and processing technique designed for distributed computing on large datasets. It consists of two main phases: the Map phase, which processes input files/data in parallel, and the Reduce phase, which performs a summary operation on the mapped data by key.

### Status: Completed (✅)

### Objective

- Minimum viable implementation that produces the same output as a sequential MapReduce application.
- Graceful exit of all forked threads / goroutines.

### Usage

To run distributed MapReduce (default):

```
make
```

To run sequential MapReduce (for testing/benchmarking):

```
make run_seq
```

To clean up the created directories (run as part of previous targets):

```
make clean
```

### Directory Structure

- ./plugins: Directory where the compiled plugin (wordcounter) will be stored.
- ./intermediates: Directory for storing intermediate files generated during the Map phase.
- ./outputs: Directory for storing the final output files after the Reduce phase.

### Additional References:

- [MIT 6.824 Spring'20](https://www.youtube.com/watch?v=cQP8WApzIQQ&list=PLrw6a1wE39_tb2fErI4-WkMbsvGQk9_UB&ab_channel=MIT6.824%3ADistributedSystems)
