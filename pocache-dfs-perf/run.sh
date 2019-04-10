#!/bin/bash
# usage:
#   if u want to run an algorithm LRU with 12 clients, 100 files, 1000 reads
#     ./run.sh algorithm ANY LRU 12 100 1000
#   if u want to run an IO mechanism potato in contiguous layout with 12 clients, 100 files, 1000 reads
#     ./run.sh io contiguouslayout potato 12 100 1000
# it is recommended to use another script to wrap this one in order to run repeated experiments

# we focus on using 1 client to write files
# then using n_clients to read files with which observing Zipf distribution
# $n_clients
# $n_writefiles
# $n_readfiles

# algorithm or IO mode
# if targeting on algorithm, layout used, io mode will be potato
# $target

# contiguous layout or striped layout
# $layout

# algorithm: LRU, GDF, ARC, and StragglerAware
# IO mode:
#   for contiguous layout:
#     potato, vanilla, selective replication, hedged read, and parallel read only
#   for striped layout:
#     potato, vanilla
# $version

# variables used in the boot script of $namenode 
# $blocksize
# $k
# $subblocksize
# $readpolicy, 1 - Reactive, 2 - Proactive, 3 - Selective Replication
# $cachealg
# $paritycacheon
# $parallelreadonly
if [ "$1" == "help" ]
then
  echo "parameters: target layout version n_clients n_writefiles n_readfiles"
  echo "with additional: k subblocksize"
  exit
fi

namenode="namenode"
stragnode="stragnode"
target=$1
layout=$2
version=$3
has_straggler=$4
n_clients=$5
n_writefiles=$6
n_readfiles=$7

# some default settings
cachealg=4
k=6
subblocksize=1 # 1MiB
blocksize="64MiB"
n_datanodes=10

if [ "$8" != "" ]
then
  k=$8
fi
if [ "$9" != "" ]
then
  n_datanodes=$9
fi
echo $k
echo $n_datanodes

cachesize=${10}
echo "cachesize" $cachesize

if [ "$target" == "algorithm" ]
then
  layout="contiguouslayout"
  cachealg=$version

  ssh $namenode 'sed -i "s/.*dfs.replication.*/<property><name>dfs.replication<\/name><value>1<\/value><\/property>/" ~/potato_benchmark/mi_confblank/hdfs-site.xml'
  ssh $namenode 'sed -i "s/.*dfs.hedged.read.on.*/<property><name>dfs.hedged.read.on<\/name><value>false<\/value><\/property>/" ~/potato_benchmark/mi_confblank/hdfs-site.xml'

  # boot hadoop-hdfs
  ssh $namenode "cd potato_benchmark ; and ./run.sh $k $subblocksize 2 $cachealg true false $n_datanodes"

  # for result path construction
  case $cachealg in
    1)
      version="LRU"
      ;;
    2)
      version="ARC"
      ;;
    3)
      version="LFU"
      ;;
    4)
      version="StragglerAwareLRU"
      ;;
    5)
      version="StragglerAwareCache"
      ;;
  esac
else # target is IO mode
  if [ "$version" == "potato" ]
  then
    paritycacheon="true"
    parallelreadonly="false"
  fi

  if [ "$version" == "vanilla" ]
  then
    paritycacheon="false"
    parallelreadonly="false"
  fi

  if [ "$version" == "parallelreadonly" ]
  then
    paritycacheon="true"
    parallelreadonly="true"
  fi

  if [ "$version" == "hedgedread" ]
  then
    paritycacheon="false"
    parallelreadonly="false"
    ssh $namenode 'sed -i "s/.*dfs.replication.*/<property><name>dfs.replication<\/name><value>2<\/value><\/property>/" ~/potato_benchmark/mi_confblank/hdfs-site.xml'
    ssh $namenode 'sed -i "s/.*dfs.hedged.read.on.*/<property><name>dfs.hedged.read.on<\/name><value>true<\/value><\/property>/" ~/potato_benchmark/mi_confblank/hdfs-site.xml'
  else
    ssh $namenode 'sed -i "s/.*dfs.replication.*/<property><name>dfs.replication<\/name><value>1<\/value><\/property>/" ~/potato_benchmark/mi_confblank/hdfs-site.xml'
    ssh $namenode 'sed -i "s/.*dfs.hedged.read.on.*/<property><name>dfs.hedged.read.on<\/name><value>false<\/value><\/property>/" ~/potato_benchmark/mi_confblank/hdfs-site.xml'
  fi

  readpolicy=2
  if [ "$version" == "selectivereplication" ]
  then
    paritycacheon="true"
    parallelreadonly="false"
    readpolicy=20
    cachealg=20
  fi

  # boot hadoop-hdfs
  ssh $namenode "cd potato_benchmark ; and ./run.sh $k $subblocksize $readpolicy $cachealg $paritycacheon $parallelreadonly $n_datanodes"
  if [ "$layout" == "stripedlayout" ]
  then
    ssh $namenode "cd potato_benchmark ; and ./enable_striped_layout.sh $k"
  fi
fi

# prepare the result path
result_path="/home/potato/dfs-perf/result/${has_straggler}/${target}/${layout}/${version}/${n_clients}_${n_writefiles}_${n_readfiles}_${blocksize}_${k}_${cachesize}"
echo $result_path
sed -i "s/.*export DFS_PERF_OUT_DIR.*/export DFS_PERF_OUT_DIR=\/home\/potato\/dfs-perf\/result\/${has_straggler}\/${target}\/${layout}\/${version}\/${n_clients}_${n_writefiles}_${n_readfiles}_${blocksize}_${k}_${cachesize}/" ./conf/dfs-perf-env.sh

# run the write file
cp conf/slaves_1 conf/slaves
sed -i "s/.*files.per.thread.*/<name>files.per.thread<\/name><value>${n_writefiles}<\/value>/" conf/testsuite/SimpleWrite.xml
sed -i "s/.*file.length.bytes.*/<name>file.length.bytes<\/name><value>$(( $k * 64 * 1024 * 1024 ))<\/value>/" conf/testsuite/SimpleWrite.xml
./bin/dfs-perf-clean
./bin/dfs-perf SimpleWrite
./bin/dfs-perf-collect SimpleWrite

# start straggler before reading file
for i in {11..22}; do
  ssh node$i "killall stress"
done
if [ "$has_straggler" == "single" ]
then
  ssh $stragnode "stress -i 1" > /dev/null &
elif [ "$has_straggler" == "vary" ]
then
  echo "straggler" $has_straggler
  python straggler_generator.py &
  straggler_generator_pid=$!
  echo "generator pid" $straggler_generator_pid
fi

# run the read file process
if [ "${n_writefiles}" == "1" ]
then
  sed -i "s/.*read.mode.*/<name>read.mode<\/name><value>RANDOM<\/value>/" ./conf/testsuite/SimpleRead.xml
else
  sed -i "s/.*read.mode.*/<name>read.mode<\/name><value>Zipf<\/value>/" ./conf/testsuite/SimpleRead.xml
fi

cp conf/slaves_${n_clients} conf/slaves
sed -i "s/.*files.per.thread.*/<name>files.per.thread<\/name><value>$(( ${n_readfiles} / ${n_clients} / 1 ))<\/value>/" ./conf/testsuite/SimpleRead.xml
./bin/dfs-perf SimpleRead
./bin/dfs-perf-collect SimpleRead

kill $straggler_generator_pid
for i in {11..22}; do
  ssh node$i "killall stress"
done

# copy the logs from namenode and datanode to local
workers=$(ssh $namenode "cat ~/hadoop-3.1.1/etc/hadoop/workers") mkdir -p ${result_path}/${namenode}/conf
scp -r $namenode:~/hadoop-3.1.1/logs/* $result_path/$namenode/ &
scp -r $namenode:~/hadoop-3.1.1/etc/hadoop/* $result_path/$namenode/conf/ &
for worker in $workers
do
  mkdir $result_path/$worker
  scp -r $worker:~/hadoop-3.1.1/logs/* $result_path/$worker/ &
done
wait
