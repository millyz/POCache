# POCache

## Introduction
POCache is a parity-only caching design that provides robust straggler
tolerance. It is a prototype atop Hadoop 3.1 HDFS, while preserving the
performance and functionalities of normal HDFS operations. To limit the erasure
coding overhead, POCache slices blocks into smaller subblocks and parallelizes
the coding operations at the subblock level.  Also, it leverages a
straggler-aware cache algorithm that takes into account both file access
popularity and straggler estimation to decide what parity blocks should be
cached.

## Build

Environment:
* Ubuntu 16.04
* JDK1.8.0_151

### Install Prerequisite
#### Maven
```
$ sudo apt-get install maven
```
#### Intel ISA-L Library
Download and install ISA-L following https://github.com/01org/isa-l

#### Protobuf
As Hadoop project requires protoc 2.5.0, please compile it by yourself:
```
wget https://github.com/google/protobuf/releases/download/v2.5.0/protobuf-2.5.0.tar.gz
tar zxvf protobuf-2.5.0.tar.gz
cd protobuf-2.5.0
./autogen.sh
./configure
make
```
Add src/protoc into your path so that the building procedure can find it.

### Build and Install

#### POCache - based on Hadoop 3.1.1
```
$ mvn package -DskipTests -Dtar -Dmaven.javadoc.skip=true -Drequire.isal -Pdist,native -DskipShade -e
```
#### pocache-dfs-perf

Please install HDFS packages in the maven repo first in order to build pocache-dfs-perf with the modified HDFS client.
```
$ mvn install -DskipTests -Dtar -Dmaven.javadoc.skip=true -Drequire.isal -Pdist,native -DskipShade -e
$ cd pocache-dfs-perf
$ mvn install
```
The documents of pocache-dfs-perf can be found under pocache-dfs-perf/docs

### Deployment
1. Distribute the releases of POCache and pocache-dfs-perf to the cluster.
   Make sure to distribute the release using rsync with all client nodes.
2. Configure and distribute the configuration files in pocache_conf to the hadoop configuration files.
3. Run Hadoop following the tutorial of the official document.
4. Use pocache-dfs-perf to benchmark POCache following the tutorial in pocache-dfs-perf.

Note that pocache-dfs-perf leverage **vmtouch** to disable page cache (by cleaning page cache before each read request) for a fair comparison environment.

## Contact
Please email to Mi Zhang (mzhang@cse.cuhk.edu.hk) if you have any questions.

## Publication
Mi Zhang, Qiuping Wang, Zhirong Shen, and [Patrick P. C. Lee](http://www.cse.cuhk.edu.hk/~pclee).

"Parity-Only Caching for Robust Straggler Tolerance", MSST 2019
