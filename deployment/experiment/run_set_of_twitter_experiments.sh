#/bin/bash

metaName="$1"

baseConfigFile="$2"
outDir="/hdd1/dyna-mast-exp"
metaOutDir="/hdd1/dyna-mast-exp-meta"
numRuns=3

#defaults

bench_num_clients=1

partition_type="ROW"

twitter_num_users=10000
twitter_num_tweets=100000

twitter_tweet_skew=1.00
twitter_follow_skew=1.75

twitter_max_follow_per_user=100
twitter_max_tweets_per_user=100

twitter_limit_tweets=1000
twitter_limit_followers=1000

twitter_limit_tweets_for_uid=100

twitter_account_partition_size=100

twitter_get_tweet_prob=10
twitter_get_tweets_from_following_prob=8
twitter_get_followers_prob=20
twitter_get_user_tweets_prob=30
twitter_insert_tweet_prob=15
twitter_get_recent_tweets_prob=7
twitter_get_tweets_from_followers_prob=8
twitter_get_tweets_like_prob=2

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

  echo "--bench_num_clients=$bench_num_clients" >> $outfile

  echo "--partition_type=$partition_type" >> $outfile

  echo "--twitter_num_users=$twitter_num_users" >> $outfile
  echo "--twitter_num_tweets=$twitter_num_tweets" >> $outfile

  echo "--twitter_tweet_skew=$twitter_tweet_skew" >> $outfile
  echo "--twitter_follow_skew=$twitter_follow_skew" >> $outfile

  echo "--twitter_max_follow_per_user=$twitter_max_follow_per_user" >> $outfile
  echo "--twitter_max_tweets_per_user=$twitter_max_tweets_per_user" >> $outfile

  echo "--twitter_limit_tweets=$twitter_limit_tweets" >> $outfile
  echo "--twitter_limit_followers=$twitter_limit_followers" >> $outfile

  echo "--twitter_limit_tweets_for_uid=$twitter_limit_tweets_for_uid" >> $outfile

  echo "--twitter_account_partition_size=$twitter_account_partition_size" >> $outfile

  echo "--twitter_get_tweet_prob=$twitter_get_tweet_prob" >> $outfile
  echo "--twitter_get_tweets_from_following_prob=$twitter_get_tweets_from_following_prob" >> $outfile
  echo "--twitter_get_followers_prob=$twitter_get_followers_prob" >> $outfile
  echo "--twitter_get_user_tweets_prob=$twitter_get_user_tweets_prob" >> $outfile
  echo "--twitter_insert_tweet_prob=$twitter_insert_tweet_prob" >> $outfile
  echo "--twitter_get_recent_tweets_prob=$twitter_get_recent_tweets_prob" >> $outfile
  echo "--twitter_get_tweets_from_followers_prob=$twitter_get_tweets_from_followers_prob" >> $outfile
  echo "--twitter_get_tweets_like_prob=$twitter_get_tweets_like_prob" >> $outfile
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

#twitter_get_tweet_prob // OLTP 0.4
#twitter_get_tweets_from_following_prob // OLAP 0.1
#twitter_get_followers_prob // OLAP 0.25
#twitter_get_user_tweets_prob // OLAP  0.4
#twitter_insert_tweet_prob // OLTP 0.6
#twitter_get_recent_tweets_prob // OLAP 0.1
#twitter_get_tweets_from_followers_prob // OLAP 0.1
#twitter_get_tweets_like_prob // OLAP 0.05


runVaryExperiment() {
  for layout in "ROW" "SORTED_COLUMN" "SORTED_MULTI_COLUMN" "COLUMN" "MULTI_COLUMN"
  do
    partition_type="$layout"
    for num_cli in "1" "4" "12" # "24"
    do
      for wkld_mix in "OLTP" "BAL" "OLAP"
      do

        if [[ "$wkld_mix" == "BAL" ]]; then
            twitter_get_tweet_prob=20
            twitter_get_tweets_from_following_prob=5
            twitter_get_followers_prob=13
            twitter_get_user_tweets_prob=20
            twitter_insert_tweet_prob=30
            twitter_get_recent_tweets_prob=5
            twitter_get_tweets_from_followers_prob=5
            twitter_get_tweets_like_prob=2
        elif [[ "$wkld_mix" == "OLTP" ]]; then
            twitter_get_tweet_prob=36
            twitter_get_tweets_from_following_prob=1
            twitter_get_followers_prob=2
            twitter_get_user_tweets_prob=4
            twitter_insert_tweet_prob=54
            twitter_get_recent_tweets_prob=1
            twitter_get_tweets_from_followers_prob=1
            twitter_get_tweets_like_prob=1
        elif [[ "$wkld_mix" == "OLAP" ]]; then
            twitter_get_tweet_prob=4
            twitter_get_tweets_from_following_prob=9
            twitter_get_followers_prob=23
            twitter_get_user_tweets_prob=36
            twitter_insert_tweet_prob=6
            twitter_get_recent_tweets_prob=9
            twitter_get_tweets_from_followers_prob=9
            twitter_get_tweets_like_prob=4
        else
          echo "Unknown workload mix!!!"
        fi

      bench_num_clients="$num_cli"
      currentName="${locName}-LAYOUT_${partition_type}-NUMCLIENTS_${bench_num_clients}-WKLD_${wkld_mix}"
      runExperiment $currentName

      rm -rf /hdd1/dyna-mast-out/*
      sudo rm -rf /tmp/core*
    done
  done
done
}

currentName="$metaName"
locName="$metaName"

for num_users in "10000"
do
  twitter_num_users="$num_users"
  for num_tweets in "100000" #
  do
    tpcc_num_tweets="$num_tweets"

    locName="${metaName}-NUSERS_${twitter_num_users}-NTWEETS_${tpcc_num_tweets}"
    runVaryExperiment
  done # items
done # warehouses

currentName="$metaName"
locName="$metaName"

