import subprocess
import random 
import time
random.seed(123456)

namenode = "namenode"
workers = subprocess.check_output(['ssh', namenode, 'cat ~/hadoop-3.1.1/etc/hadoop/workers']).split('\n')[:-1]

cnt = 0
while 1:
    threads = []
    stragglers = []
    for worker in workers:
        rd = random.randint(1, 100)
        if rd <= 5:
            stragglers.append(worker)
            threads.append(subprocess.Popen(['ssh', worker, 'stress -i 1 -t 900']))
    print stragglers
    time.sleep(900)
    for thread in threads:
        subprocess.Popen.wait(thread)

