#/bin/bash

indir="/hdd1/dyna-mast/Adapt-HTAP/"
outfile="$indir/code/src/common/perf_tracking_defs.h"

# To generate the list of timers
 # get the existing definitions of timers
cat $indir/code/src/common/perf_tracking_defs.h  | grep TIMER_ID | grep define | awk '{print $2}'> /tmp/def-timer.txt
# first get the timers used in the code

grep -r TIMER_ID $indir/code/src/ | grep -v perf_tracking | egrep -o "[_A-Z]*_TIMER_ID" | sort | uniq  > /tmp/dm-timer-ids.txt
#cat /tmp/dm-timer-ids.txt > /tmp/def-timer.txt
# then get the timers in the order in which they are in the code
cat /tmp/def-timer.txt | xargs -I {} sh -c "grep '^{}$' /tmp/dm-timer-ids.txt" > /tmp/ordered-timer-ids.txt
# build up the id's
echo "#pragma once" > $outfile
echo " " >> $outfile
echo "// definitions" >> $outfile
cat /tmp/ordered-timer-ids.txt  | awk '{print "#define " $0 " " NR}' >> $outfile
numlines=$( cat /tmp/ordered-timer-ids.txt | wc -l)
echo " " >> $outfile
echo "#define END_TIMER_COUNTERS $numlines" >> $outfile
# get the to string version

echo " " >> $outfile
echo "// strings" >> $outfile
echo "static const std::string timer_names[END_TIMER_COUNTERS + 1] = {" >> $outfile
echo "     \"off_by_one_stub\"," >> $outfile
cat /tmp/ordered-timer-ids.txt  | rev | cut -d "_" -f 3- | rev | tr '[:upper:]' '[:lower:]' | sed "s/^/     \"/g" | sed "s/$/\",/g"  >> $outfile
echo "};" >> $outfile


# get the -1 for model information
echo " " >> $outfile
echo "// model information" >> $outfile
echo "static const int64_t model_timer_positions[END_TIMER_COUNTERS + 1] = {" >> $outfile
echo "     -1 /* off_by_one_stub */," >>  $outfile
cat /tmp/ordered-timer-ids.txt  | rev | cut -d "_" -f 3- | rev | tr '[:upper:]' '[:lower:]' | sed "s/^/     -1 \/* /g" | sed "s/$/ *\/,/g"  >> $outfile
echo "};" >> $outfile

