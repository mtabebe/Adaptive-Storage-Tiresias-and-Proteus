#/bin/bash

metaName="$1"

baseConfigFile="$2"
outDir="/hdd1/dyna-mast-exp"
metaOutDir="/hdd1/dyna-mast-exp-meta"
numRuns=3

#defaults

tpcc_num_clients=1
tpch_num_clients=1

partition_type="ROW"

tpcc_num_warehouses=2
tpcc_num_items=10000
tpcc_num_suppliers=10000

tpcc_item_partition_size=1000
tpcc_partition_size=100
tpcc_district_partition_size=1
tpcc_customer_partition_size=30
tpcc_supplier_partition_size=1000

tpcc_new_order_prob=45
tpcc_payment_prob=45
tpcc_stock_level_prob=10
tpcc_delivery_prob=0
tpcc_order_status_prob=0
tpch_all_prob=100

tpcc_spec_num_districts_per_warehouse=5
tpcc_spec_num_customers_per_district=900
tpcc_spec_initial_num_customers_per_district=300


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

  echo "--tpcc_num_clients=$tpcc_num_clients" >> $outfile
  echo "--tpch_num_clients=$tpch_num_clients" >> $outfile

  echo "--tpcc_customer_part_type=$partition_type" >> $outfile
  echo "--tpcc_customer_district_part_type=$partition_type" >> $outfile
  echo "--tpcc_history_part_type=$partition_type" >> $outfile
  echo "--tpcc_item_part_type=$partition_type" >> $outfile
  echo "--tpcc_nation_part_type=$partition_type" >> $outfile
  echo "--tpcc_new_order_part_type=$partition_type" >> $outfile
  echo "--tpcc_order_part_type=$partition_type" >> $outfile
  echo "--tpcc_order_line_part_type=$partition_type" >> $outfile
  echo "--tpcc_region_part_type=$partition_type" >> $outfile
  echo "--tpcc_stock_part_type=$partition_type" >> $outfile
  echo "--tpcc_supplier_part_type=$partition_type" >> $outfile
  echo "--tpcc_warehouse_part_type=$partition_type" >> $outfile

  echo "--tpcc_num_warehouses=$tpcc_num_warehouses" >> $outfile
  echo "--tpcc_num_items=$tpcc_num_items" >> $outfile
  echo "--tpcc_num_suppliers=$tpcc_num_suppliers" >> $outfile

  echo "--tpcc_item_partition_size=$tpcc_item_partition_size" >> $outfile
  echo "--tpcc_partition_size=$tpcc_partition_size" >> $outfile
  echo "--tpcc_district_partition_size=$tpcc_district_partition_size" >> $outfile
  echo "--tpcc_customer_partition_size=$tpcc_customer_partition_size" >> $outfile
  echo "--tpcc_supplier_partition_size=$tpcc_supplier_partition_size" >> $outfile

  echo "--tpcc_new_order_prob=$tpcc_new_order_prob" >> $outfile
  echo "--tpcc_payment_prob=$tpcc_payment_prob" >> $outfile
  echo "--tpcc_stock_level_prob=$tpcc_stock_level_prob" >> $outfile
  echo "--tpcc_delivery_prob=$tpcc_delivery_prob" >> $outfile
  echo "--tpcc_order_status_prob=$tpcc_order_status_prob" >> $outfile
  echo "--tpch_all_prob=$tpch_all_prob" >> $outfile

  echo "--tpcc_spec_num_districts_per_warehouse=$tpcc_spec_num_districts_per_warehouse" >> $outfile
  echo "--tpcc_spec_num_customers_per_district=$tpcc_spec_num_customers_per_district" >> $outfile
  echo "--tpcc_spec_initial_num_customers_per_district=$tpcc_spec_initial_num_customers_per_district" >> $outfile
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
  for layout in "ROW" "SORTED_COLUMN" "SORTED_MULTI_COLUMN" "COLUMN" "MULTI_COLUMN"
  do
    partition_type="$layout"
    for num_cli in "1-0" "4-0" "12-0" # "0-1" "0-4" "0-12" "1-12" "12-1" "4-4" "8-4" "4-8"
    do
      tpcc_num_clients=$(echo $num_cli | cut -d "-" -f 1 )
      tpch_num_clients=$(echo $num_cli | cut -d "-" -f 2 )
      currentName="${locName}-LAYOUT_${partition_type}-C_CLIENTS_${tpcc_num_clients}-H_CLIENTS_${tpch_num_clients}"
      runExperiment $currentName

      rm -rf /hdd1/dyna-mast-out/*
      sudo rm -rf /tmp/core*

    done # num cli
  done # layout
}



currentName="$metaName"
locName="$metaName"

for num_wh in "1" "5"
do
  tpcc_num_warehouses="$num_wh"
  for num_items in "10000" #
  do
    tpcc_num_items="$num_items"

    locName="${metaName}-NWhouse_${tpcc_num_warehouses}-NItems-${tpcc_num_items}"
    runVaryExperiment
  done # items
done # warehouses

currentName="$metaName"
locName="$metaName"

