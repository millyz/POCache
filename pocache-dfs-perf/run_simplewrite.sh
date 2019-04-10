#!/bin/bash
blksz="64M"
testcase="SimpleWrite"
version=$1
num_clients=$2


function run () {
  echo $version, $testcase, $k, $pcache_on, $parallel_read_only, $filesize, $time
  sed -i "s/.*file.length.bytes.*/<name>file.length.bytes<\/name><value>$filesize<\/value>/" ./conf/testsuite/${testcase}.xml
  sed -i "s/.*files.per.thread.*/<name>files.per.thread<\/name><value>$(( 1000 / $num_clients / 1 ))<\/value>/" ./conf/testsuite/SimpleRead.xml
  sed -i "s/.*export DFS_PERF_OUT_DIR.*/export DFS_PERF_OUT_DIR=\"\$DFS_PERF_HOME\/result\/${num_clients}clients\/${version}\/blksz${blksz}_stripesz_${k}_filesz${filesz}M\"/" ./conf/dfs-perf-env.sh
  if [ "version" == "hedgedread" ]
  then
    ssh node11 "cd ~/potato_benchmark ; ./run1.sh $k 1 2 4 false false 10"
  else
    ssh node11 "cd ~/potato_benchmark ; ./run.sh $k 1 2 4 $pcache_on $parallel_read_only 10"
  fi
  ./bin/dfs-perf-clean
  ./bin/dfs-perf $testcase
  ./bin/dfs-perf-collect $testcase
  #bash ./run_simpleread.sh
}

for k in 6
do
#for version in "original"
#"ours" "parallel_read_only" 
#do 
  parallel_read_only="false"
  if [ "$version" == "ours" ]
  then
    pcache_on="true"
  elif [ "$version" == "parallel_read_only" ]
  then
    pcache_on="true"
    parallel_read_only="true"
  else
    pcache_on="false"
  fi

  filesz=$(( ${k} * 64))
  #for filesz in 256 512 1024
  #do
  filesize=$(( ${filesz}*1024*1024 ))
  for time in 600
  do
    run
  done
done
#done
