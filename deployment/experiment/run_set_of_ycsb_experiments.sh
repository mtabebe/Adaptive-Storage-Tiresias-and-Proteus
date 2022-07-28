#/bin/bash

metaName="$1"

baseConfigFile="$2" # "../tmp-configs/base_ycsb.cfg"
outDir="/hdd1/dyna-mast-exp"
metaOutDir="/hdd1/dyna-mast-exp-meta"
numRuns=3

#defaults

ycsb_zipf_alpha=0
ycsb_scan_prob=50
ycsb_multi_rmw_prob=100
bench_num_clients=1

ycsb_partition_size=5000
ycsb_column_partition_size=4

ycsb_partition_type="ROW"

now=$(date +%b-%d-%H:%M:%S)
metaDir="$metaOutDir/$metaName"
metaDirNow="$metaDir-$now"
mkdir -p $metaDirNow
ln -sfn $metaDirNow $metaDir

cp $0 $metaDir/$0
cp $baseConfigFile $metaDir/base_ycsb.cfg
manifestFile="$metaDir/manifest.txt"
touch $manifestFile

generateFlags() {
  outfile="$1"

  cat $baseConfigFile > $outfile

  echo "--ycsb_zipf_alpha=$ycsb_zipf_alpha" >> $outfile
  echo "--ycsb_scan_prob=$ycsb_scan_prob" >> $outfile
  echo "--ycsb_multi_rmw_prob=$ycsb_multi_rmw_prob" >> $outfile

  echo "--bench_num_clients=$bench_num_clients" >> $outfile

  echo "--ycsb_partition_size=$ycsb_partition_size" >> $outfile
  echo "--ycsb_column_partition_size=$ycsb_column_partition_size" >> $outfile
  echo "--ycsb_partition_type=$ycsb_partition_type" >> $outfile
}

runExperiment() {
  shortname="$1"

  echo "running $shortname"

  now=$(date +%b-%d-%H:%M:%S)
  local_out="$outDir/$shortname"
  local_out_now="$local_out-$now"

  echo $local_out_now >> $manifestFile

  mkdir -p $local_out_now
  ln -sfn $local_out_now $local_out

  flagFile="$local_out/generated_flags.cfg"

  generateFlags $flagFile
  ./run_single_ycsb_experiment.sh $flagFile $local_out $shortname $numRuns
}


runVaryExperiment() {
  for layout in "ROW" "COLUMN" "MULTI_COLUMN" "SORTED_COLUMN" "SORTED_MULTI_COLUMN"
  do
    ycsb_partition_type="$layout"
    for num_cli in "1" "4" "12" # "24"
    do
      bench_num_clients="$num_cli"
      currentName="${locName}-LAYOUT_${ycsb_partition_type}-PSIZE_${ycsb_partition_size}-CSIZE_${ycsb_column_partition_size}-ZIPF_${ycsb_zipf_alpha}-SCAN_${ycsb_scan_prob}-MKRMW_${ycsb_multi_rmw_prob}-NUMCLIENTS_${bench_num_clients}"
      runExperiment $currentName

      rm -rf /hdd1/dyna-mast-out/*
      sudo rm -rf /tmp/core*

    done # num cli
  done # layout
}



currentName="$metaName"
locName="$metaName"

#for part_size in "5000"
#do
#  ycsb_partition_size="$part_size"
#  for col_size in "4"
#  do
#    ycsb_column_partition_size="$col_size"
#    for zipf in "0.0"
#    do
#      ycsb_zipf_alpha=$zipf
#      for scan_pair in "50-50"
#      do
#          ycsb_scan_prob=$(echo $scan_pair | cut -d "-" -f 1 )
#          ycsb_multi_rmw_prob=$(echo $scan_pair | cut -d "-" -f 2 )
#
#          locName="${metaName}-ITER"
#          runVaryExperiment
#      done # scan
#    done # zipf
#  done # col part size
#done # part_size
#
#exit

#for part_size in "50" "500" "5000"
#do
#  ycsb_partition_size="$part_size"
#  for col_size in "4"
#  do
#    ycsb_column_partition_size="$col_size"
#    for zipf in "0.0" "0.75"
#    do
#      ycsb_zipf_alpha=$zipf
#      for scan_pair in "50-50" "10-90"
#      do
#          ycsb_scan_prob=$(echo $scan_pair | cut -d "-" -f 1 )
#          ycsb_multi_rmw_prob=$(echo $scan_pair | cut -d "-" -f 2 )
#
#          locName="${metaName}-UPDATE-HEAVY"
#          runVaryExperiment
#      done # scan
#    done # zipf
#  done # col part size
#done # part_size

for part_size in "5000"
do
  ycsb_partition_size="$part_size"
  for col_size in "4" #"1" "4" # "2"
  do
    ycsb_column_partition_size="$col_size"
    for zipf in "0.0"
    do
      ycsb_zipf_alpha=$zipf
      for scan_pair in "90-10" "50-50"
      do
          ycsb_scan_prob=$(echo $scan_pair | cut -d "-" -f 1 )
          ycsb_multi_rmw_prob=$(echo $scan_pair | cut -d "-" -f 2 )

          locName="${metaName}-SCAN-HEAVY"
          runVaryExperiment
      done # scan
    done # zipf
  done # col part size
done # part_size
