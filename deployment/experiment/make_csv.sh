#/bin/bash

infile="$1"
outlabel="$2"

outdir="/hdd1/dyna-mast-exp-csvs/${outlabel}"
outfile="/hdd1/dyna-mast-exp-csvs/${outlabel}/csv.txt"

mkdir -p $outdir

cat $infile > $outdir/input.txt

firstLine=$(cat $infile | head -n 1)

variable=$(echo $firstLine | cut -d "," -f 1)
label=$(echo $firstLine | cut -d "," -f 2)

echo "$variable,$label,$label-CI" > $outfile

cat $infile | tail -n +2 | while read line
do
  var=$(echo $line | cut -d "," -f 1)
  indir=$(echo $line | cut -d "," -f 2)
  tps_line=$(cat $indir/total_tps.txt)
  echo "$var,$tps_line" >> $outfile

done
