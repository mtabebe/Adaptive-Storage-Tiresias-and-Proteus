#/bin/bash

inputDir="$1"

pyScript="exp_utils.py"
cp $pyScript $inputDir/input/$pyScript

allCategories=$inputDir/result_categories.txt
cat $inputDir/results/* | cut -d ":" -f 1 | sort | uniq > $allCategories

outFile="$inputDir/results.txt"
touch $outFile
rm $outFile
touch $outFile

while read -r line; do
  cat $inputDir/results/* | grep "^ $line:" | cut -d ":" -f 2- | cut -d ' ' -f 1 > $inputDir/tmp.txt
  results=$(python3 $pyScript $inputDir/tmp.txt)
  avg=$( echo "$results" | cut -d "," -f 1 )
  ci=$( echo "$results" | cut -d "," -f 2 )
  echo "$line,$avg,$ci" >> $outFile
done < $allCategories
