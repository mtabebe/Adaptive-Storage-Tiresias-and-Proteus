#/bin/bash

baseConfigFile="$1"
outDir="$2"
shortName="$3"
numRuns=$4

verbosity=1
benchScript="../scripts/single_site_bench.sh"
execFile="../../code/build/exe/single_site_benchmark/release/single_site_benchmark"

mkdir -p $outDir

mkdir -p $outDir/input

echo "$0 $baseConfigFile $outDir $shortName $numRuns" > $outDir/input/real_run.sh
echo "$0 baseConfigFile $outDir $shortName $numRuns" > $outDir/input/run.sh
cp $0 $outDir/input/$0
cp $baseConfigFile $outDir/input/baseConfigFile
cp mergeResults.sh $outDir/input/mergeResults.sh

cat $baseConfigFile > $outDir/flags.cfg

mkdir -p $outDir/logs
mkdir -p $outDir/flags
mkdir -p $outDir/constants
mkdir -p $outDir/results

for ((i=0;i<$numRuns;i++))
do
  echo $shortName $i
  $benchScript -c $outDir/flags.cfg -s $execFile -v $verbosity

  baseFile="/hdd1/dyna-mast-out/single_site_bench"
  cp "${baseFile}_logs.log" "$outDir/logs/$i.txt"
  cp "${baseFile}_flags.cfg" "$outDir/flags/$i.txt"
  cp "${baseFile}_constants.cfg" "$outDir/constants/$i.txt"
  cp "${baseFile}_results.res" "$outDir/results/$i.txt"
done

./mergeResults.sh $outDir
