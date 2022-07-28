set -x

ss_config="tmp-configs/tpcc_gen_ss.cfg"

./scripts/generate_tpcc.sh -c $ss_config -s /hdd1/dyna-mast/Adapt-HTAP/code/build/exe/tpcc_generator/debugNonOpt/tpcc_generator -v 3

for i in `seq 0 7`;
do
  echo $i;
  outfile="tmp-configs/tpcc_gen_ds-$i.cfg"
  cp tmp-configs/tpcc_gen_ds.cfg $outfile
  echo "--data_site_id=$i" >> $outfile

  ./scripts/generate_tpcc.sh -c $outfile -s /hdd1/dyna-mast/Adapt-HTAP/code/build/exe/tpcc_generator/debugNonOpt/tpcc_generator -v 3
done
