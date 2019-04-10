LRU=1
ARC=2
GDF=3
StragglerAware=4

cp conf/slaves_1 conf/slaves
./run_simplewrite_alg.sh $StragglerAware 1
cp conf/slaves_1 conf/slaves
./run_simpleread.sh

#cp conf/slaves_1 conf/slaves
#./run_simplewrite_alg.sh $StragglerAware 4
#cp conf/slaves_4 conf/slaves
#./run_simpleread.sh

#cp conf/slaves_1 conf/slaves
#./run_simplewrite_alg.sh $StragglerAware 8
#cp conf/slaves_8 conf/slaves
#./run_simpleread.sh

#cp conf/slaves_1 conf/slaves
#./run_simplewrite_alg.sh $StragglerAware 12
#cp conf/slaves_12 conf/slaves
#./run_simpleread.sh

cp conf/slaves_1 conf/slaves
./run_simplewrite_alg.sh $LRU 1
cp conf/slaves_1 conf/slaves
./run_simpleread.sh

#cp conf/slaves_1 conf/slaves
#./run_simplewrite_alg.sh $LRU 4
#cp conf/slaves_4 conf/slaves
#./run_simpleread.sh

#cp conf/slaves_1 conf/slaves
#./run_simplewrite_alg.sh $LRU 8
#cp conf/slaves_8 conf/slaves
#./run_simpleread.sh

#cp conf/slaves_1 conf/slaves
#./run_simplewrite_alg.sh $LRU 12
#cp conf/slaves_12 conf/slaves
#./run_simpleread.sh

cp conf/slaves_1 conf/slaves
./run_simplewrite_alg.sh $ARC 1
cp conf/slaves_1 conf/slaves
./run_simpleread.sh

#cp conf/slaves_1 conf/slaves
#./run_simplewrite_alg.sh $ARC 4
#cp conf/slaves_4 conf/slaves
#./run_simpleread.sh

#cp conf/slaves_1 conf/slaves
#./run_simplewrite_alg.sh $ARC 8
#cp conf/slaves_8 conf/slaves
#./run_simpleread.sh

#cp conf/slaves_1 conf/slaves
#./run_simplewrite_alg.sh $ARC 12
#cp conf/slaves_12 conf/slaves
#./run_simpleread.sh

cp conf/slaves_1 conf/slaves
./run_simplewrite_alg.sh $GDF 1
cp conf/slaves_1 conf/slaves
./run_simpleread.sh

#cp conf/slaves_1 conf/slaves
#./run_simplewrite_alg.sh $GDF 4
#cp conf/slaves_4 conf/slaves
#./run_simpleread.sh

#cp conf/slaves_1 conf/slaves
#./run_simplewrite_alg.sh $GDF 8
#cp conf/slaves_8 conf/slaves
#./run_simpleread.sh

#cp conf/slaves_1 conf/slaves
#./run_simplewrite_alg.sh $GDF 12
#cp conf/slaves_12 conf/slaves
#./run_simpleread.sh
