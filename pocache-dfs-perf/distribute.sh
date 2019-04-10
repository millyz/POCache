rm ~/dfs-perf/logs/*
for i in {11..22}
do
  ssh node$i "rm -rf ~/dfs-perf/logs/*" &
  ssh node$i "rm -rf ~/dfs-perf/result/*" &
  echo node$i
  rsync -a -f"- result" -f"- logs" . node$i:~/dfs-perf/ &
done

for i in {37..38}
do
  ssh node$i "rm -rf ~/dfs-perf/log/*" &
  ssh node$i "rm -rf ~/dfs-perf/result/*" &
  echo node$i
  rsync -a -f"- result" -f"- logs" . node$i:~/dfs-perf/ &
done
wait
