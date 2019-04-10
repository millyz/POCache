#!/bin/bash
blksz="64M"
testcase="SimpleRead"

./bin/dfs-perf $testcase
./bin/dfs-perf-collect $testcase
