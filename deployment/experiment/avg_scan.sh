infile="$1"
outfile="$2"

averagePy="/hdd1/dyna-mast/Adapt-HTAP/deployment/experiment/exp_utils.py"
cat $infile | grep "SCAN" | cut -d ":" -f 5- | cut -d "," -f 1-5 | sort | uniq | grep -v scan > $outfile.cats

cat $outfile.cats | while read line
do
  cat $infile | grep "$line," | rev | cut -d "," -f 1 | rev > $outfile.out
  avg=$(python $averagePy $outfile.out)
  echo "$line,$avg" >> $outfile
done
