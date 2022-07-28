# How to test on a single site

# Kafka
```
./deployment/scripts/init_kafka.sh -z -k /tmp/
./deployment/scripts/create_kafka_topics.sh -r 1 -p 3 -t dyna-mast-updates
```

# Start site selectors
```
./deployment/scripts/start_site_manager.sh -c deployment/tmp-configs/site_manager1.cfg -o /hdd1/dyna-mast-out/ -s /hdd1/dyna-mast/Adapt-HTAP/code/build/exe/site_manager_server/debugNonOpt/site_manager_server
./deployment/scripts/start_site_manager.sh -c deployment/tmp-configs/site_manager2.cfg -o /hdd1/dyna-mast-out/ -s /hdd1/dyna-mast/Adapt-HTAP/code/build/exe/site_manager_server/debugNonOpt/site_manager_server
```

## Wait for them to be initialized
```
watch tail -n 5 /hdd1/dyna-mast-out/site_manager_logs.log
```

# Start site selector
```
./deployment/scripts/start_site_selector.sh -c deployment/tmp-configs/site_selector.cfg -o /hdd1/dyna-mast-out/ -s /hdd1/dyna-mast/Adapt-HTAP/code/build/exe/site_selector_server/debugNonOpt/site_selector_server
```

## Wait for initialization
```
watch tail -n 5 /hdd1/dyna-mast-out/site_selector_logs.log
```

# Start benchmark
```
/hdd1/dyna-mast/Adapt-HTAP/deployment/scripts/start_oltpbench.sh -o /hdd1/dyna-mast-out/ -c /hdd1/dyna-mast/Adapt-HTAP/deployment/tmp-configs/hdbycsb_benchmark_config.xml
```


# Kill all
```
./deployment/scripts/kill_all_adapt_htap.sh
```


