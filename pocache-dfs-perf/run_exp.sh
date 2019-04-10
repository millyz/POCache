#!/bin/bash
IO="io"
ALG="algorithm"

LRU=1
ARC=2
LFU=3
StragglerAwareLRU=4
StragglerAwareCache=5

namenode="node11"
cachesize=100

#for cachesize in 40
#do
  #ssh $namenode "sed -i \"s/.*dfs.client.pcache.read.cache.size.*/<property><name>dfs.client.pcache.read.cache.size<\/name><value>$cachesize<\/value><\/property>/\" ~/potato_benchmark/mi_confblank/hdfs-site.xml"
  #for straggler in "vary"
  #do
    #for alg in $StragglerAwareLRU
    #do
      #./run.sh $ALG ANY $alg $straggler 1 100 2000 4 10 $cachesize
    #done
  #done
#done

straggler="single"
for cachesize in 100
do
  ssh $namenode "sed -i \"s/.*dfs.client.pcache.read.cache.size.*/<property><name>dfs.client.pcache.read.cache.size<\/name><value>$cachesize<\/value><\/property>/\" ~/potato_benchmark/mi_confblank/hdfs-site.xml"
  for n_clients in 4 8 12 ; do
    for layout in "contiguouslayout" ; do
      for version in "potato" "vanilla" "selectivereplication" "hedgedread"; do
        ./run.sh $IO $layout $version $straggler $n_clients 100 2000 4 10 $cachesize
      done
    done
    for layout in "stripedlayout" ; do
      for version in "potato" "vanilla" ; do
        ./run.sh $IO $layout $version $straggler $n_clients 100 2000 4 10 $cachesize
      done
    done
  done
done

straggler="nostraggler"
for cachesize in 100
do
  ssh $namenode "sed -i \"s/.*dfs.client.pcache.read.cache.size.*/<property><name>dfs.client.pcache.read.cache.size<\/name><value>$cachesize<\/value><\/property>/\" ~/potato_benchmark/mi_confblank/hdfs-site.xml"
  for n_clients in 4 8 12 ; do
    for layout in "contiguouslayout" ; do
      for version in "vanilla" "selectivereplication" "hedgedread" "potato" ; do
        ./run.sh $IO $layout $version $straggler $n_clients 100 2000 4 10 $cachesize
      done
    done
    for layout in "stripedlayout" ; do
      for version in "vanilla" "potato" ; do
        ./run.sh $IO $layout $version $straggler $n_clients 100 2000 4 10 $cachesize
      done
    done
  done
done
