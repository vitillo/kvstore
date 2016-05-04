# kvstore

The Log-Structure Merge (LSM) Tree is tailored for work-loads that experience high frequency inserts and updates over an extended period of time and only a moderate rate of retrieval operations.

A LSM tree organizes data in one or more layers tailored for the underlying storage medium. Data moves from the upper layers to the lower layers in batches using algorithms reminiscent of merge-sort. The LSM tree achieves high throughput by cascading data over time in batches from smaller but more expensive stores to larger and cheaper ones. 

This project implements a parallel LSM tree that partitions the key-space into multiple independent key-value stores to scale the workload with the number of cores. Each store has its own LSM tree composed of a memory table and a hierarchical set of memory-mapped sorted string tables (SSTables).

### Development 

To create a development environment:
```bash
vagrant up
vagrant ssh
```

To build the project:
```
cd /vagrant
mkdir /tmp/build  # Can't mmap files in shared vagrant directory...
cmake /vagrant
make
```

To run tests from the build directory:
```bash
./run_tests
```
