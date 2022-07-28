#/bin/bash

cd /hdd1/dyna-mast/

cd Adapt-HTAP
git reset --hard
git checkout master
git pull origin master
git fetch origin
git fetch origin master # drp-deployment #leap # chunked_scan chunked_scan_min_prop
git checkout master #drp-deployment #chunked_scan  leap chunked_scan_min_prop
git pull origin master #drp-deployment # chunked_scan leap chunked_scan_min_prop
git log | head -n 1
cd ../

cd oltpbench
git reset --hard
git checkout master
git pull origin master
git fetch origin
git fetch origin drp-oltpbench # keys_within_partitions # persist-restore-ops
git checkout drp-oltpbench # keys_within_partitions # persist-restore-ops
git pull origin drp-oltpbench # keys_within_partitions #persist-restore-ops
git log | head -n 1
cd ../

cd librdkafka;
git reset --hard
git checkout master
git pull origin master
git fetch origin
git checkout ca7a8297bd550db59d9cd003722708c4ffbb9b63;
git log | head -n 1
cd ../

cd folly;
git reset --hard
git checkout master
git pull origin master
git fetch origin
git checkout fd84bde19311f13f840b8e5809c87714575ade72 #b555e4cd5086524c422866358fbf62b4878b838f # a478d52fdeed1c3a1342615ecdc30e77ac1d9af9
git log | head -n 1
cd ../

cd gflags;
git reset --hard
git checkout master
git pull origin master
git fetch origin
git checkout 28f50e0fed19872e0fd50dd23ce2ee8cd759338e # e292e0452fcfd5a8ae055b59052fc041cbab4abf
git log | head -n 1
cd ../

cd glog;
git reset --hard
git checkout master
git pull origin master
git fetch origin
git checkout 96a2f23dca4cc7180821ca5f32e526314395d26a # 2faa186e62d544e930305ffd8f8e507b2054cc9b
git log | head -n 1
cd ../

cd pytorch;
git reset --hard
git checkout master
git pull origin master
git fetch origin
git checkout e1a7ec3c4f925670bb1adcb52beec27ca8d2c490 
git log | head -n 1
cd ../


# googletest
# 5ba69d5cb93779fba14bf438dfdaf589e2b92071

