cp conf/slaves_1 conf/slaves
./run_simplewrite.sh original 4
cp conf/slaves_4 conf/slaves
./run_simpleread.sh

cp conf/slaves_1 conf/slaves
./run_simplewrite.sh original 8
cp conf/slaves_8 conf/slaves
./run_simpleread.sh

cp conf/slaves_1 conf/slaves
./run_simplewrite.sh original 12
cp conf/slaves_12 conf/slaves
./run_simpleread.sh

cp conf/slaves_1 conf/slaves
./run_simplewrite.sh hedgedread 4
cp conf/slaves_4 conf/slaves
./run_simpleread.sh

cp conf/slaves_1 conf/slaves
./run_simplewrite.sh hedgedread 8
cp conf/slaves_8 conf/slaves
./run_simpleread.sh

cp conf/slaves_1 conf/slaves
./run_simplewrite.sh hedgedread 12
cp conf/slaves_12 conf/slaves
./run_simpleread.sh

cp conf/slaves_1 conf/slaves
./run_simplewrite.sh parallel_read_only 4
cp conf/slaves_4 conf/slaves
./run_simpleread.sh

cp conf/slaves_1 conf/slaves
./run_simplewrite.sh parallel_read_only 8
cp conf/slaves_8 conf/slaves
./run_simpleread.sh

cp conf/slaves_1 conf/slaves
./run_simplewrite.sh parallel_read_only 12
cp conf/slaves_12 conf/slaves
./run_simpleread.sh

