#/bin/bash

indir="$1"

tps=$(cat $indir/results.txt | grep "TPS" | grep SUCCESS | cut -d "," -f 2 | awk '{sum+=$1} END {print sum}')
ci=$(cat $indir/results.txt | grep "TPS" | grep SUCCESS | cut -d "," -f 3 | awk '{sum+=$1} END {print sum}')

if [ -z "$tps" ]
then
  tps=0
fi

if [ -z "$ci" ]
then
  ci=0
fi

echo "$tps,$ci" > $indir/total_tps.txt
