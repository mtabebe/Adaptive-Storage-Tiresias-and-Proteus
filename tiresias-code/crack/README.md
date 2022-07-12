Stochastic Database Cracking [![Build Status](https://travis-ci.org/felix-halim/scrack.svg?branch=master)](https://travis-ci.org/felix-halim/scrack)
======

This is the source codes for the experiments in the 
[Stochastic Database Cracking: Towards Robust Adaptive Indexing in Main-Memory Column-Stores](http://vldb.org/pvldb/vol5/p502_felixhalim_vldb2012.pdf) paper.

To run a particular algorithm on a particular dataset, execute:

    ./run.sh [data] [algo] [nqueries] [workload] [selectivity] [update] [timelimit]


\[data\] is one of the following:
- 100000000.data
- skyserver.data (it will be downloaded on demand around 2.2GB)

\[algo\] is one of the following:
- crack
- sort
- scan
- ddc
- ddr
- dd1c
- dd1r
- mdd1r
- mdd1rp1
- mdd1rp5
- mdd1rp10
- mdd1rp50
- naive_r1th
- naive_r2th
- naive_r4th
- naive_r8th
- naive_r1x
- naive_r2x
- aicc
- aicc1r
- aics
- aics1r
- aiss

\[nqueries\] is an integer denoting the number of queries to be executed.

\[workload\] is one of the following:
- Random
- Sequential
- SeqNoOver
- SeqRevOver
- SeqAlternate
- SeqRand
- ZoomIn
- SeqZoomIn
- ZoomOut
- SeqZoomOut
- Skew
- ConsRandom
- SkyServer (downloaded on demand)

\[selectivity\] is a floating point, e.g.:
- 0.5 (means 50% selectivity)
- 1e-2 (means 1% selectivity)
- 1e-7 (means 0.00001% selectivity)

\[update\] is one of the following:
- NOUP (means read only queries)
- LFHV (means low frequency high volume updates)
- HFLV (means high frequency low volume updates)
- ROLL (means queue-like update workload)
- TRASH (means insert 1M tuples at 10, 10^5 th query)
- DELETE (means delete 1000 tuples every 1000 queries)
- APPEND (means gradually insert 10M queries every 1000 queries)

\[timelimit\] is an integer denoting the maximum runtime in seconds before it is prematurely terminated (if exceeded).

Example run:

    ./run.sh 100000000.data crack 100000 Random 1e-2 NOUP 30

SkyServer dataset and queries
======

The SkyServer dataset consists of a sequence of 585634221 integers which represents the degree of ascension
in the Photoobjall table. Originally the degree is a floating point between 0 to 360, but in this dataset,
it has been multiplied by 1 million and converted to integers.
You will need to download the SkyServer dataset from
[scrack-skyserver-dataset](https://github.com/felix-halim/scrack-skyserver-dataset) repository.
Then you will be able to run:

    ./run.sh skyserver.data dd1r 200000 SkyServer 1e-7 NOUP 60
    ./run.sh skyserver.data dd1r 200000 SkyServer 1e-7 HFLV 60

The SkyServer queries consist of a sequence of 158325 point queries on the ascension column
(similarly formatted by multiplying it by 1 million and converted to integers).
