# How To Setup A Cluster

## Setup Machines
* Reserve a set of machines list them in a configuration file, say */hdd1/dyna-mast/horizondb/deployment/tmp-configs/tpcc-machines.txt*
* Become the user, in this case mtabebe
  * `sudo su - mtabebe`
* Ensure you can ssh to each machine:
  * `./ssh_script.sh -c ../tmp-configs/tpcc-machines.txt` repeat adding to the *known_hosts* file
  * run a sample command `./run_command_on_all_hosts.sh -c ../tmp-configs/tpcc-machines.txt "ls /hdd1/"`
* Setup to run the setup script: `cp setup_machine.sh ~/hdb/`
* Setup the root directory:
  * `./run_command_on_all_hosts.sh -c ../tmp-configs/tpcc-machines.txt "/home/mtabebe/hdb/setup_machine.sh -b 1 -e 1 -r /hdd1/dyna-mast"`
  * `./run_command_on_all_hosts.sh -c ../tmp-configs/tpcc-machines.txt "/home/mtabebe/hdb/setup_machine.sh -b 1 -e 1 -r /hdd1/dyna-mast-out"`
* Setup the machines:
  * `./run_command_on_all_hosts.sh -c ../tmp-configs/tpcc-machines.txt "/home/mtabebe/hdb/setup_machine.sh -b 0"`
* Update limits:
  * `cat /hdd1/dyna-mast/horizondb/deployment/configs/limits.conf >  /home/mtabebe/hdb/limits.conf`
  * `./scripts/run_command_on_all_hosts.sh -c tmp-configs/tpcc-machines.txt "sudo bash -c  'cat /home/mtabebe/hdb/limits.conf > /etc/security/limits.conf'"`
* Make sure things are correctly set up:
  * `./horizondb/code/build/exe/unitTest/release/unitTest`
* Install packages for Sentinel
  ```
  ./run_command_on_all_hosts.sh -c ../tmp-configs/tpcc-machines.txt "sudo apt-get --yes install python3-pip python3-dev"
  ./run_command_on_all_hosts.sh -c ../tmp-configs/tpcc-machines.txt "sudo pip3 install --upgrade pip"
  ./run_command_on_all_hosts.sh -c ../tmp-configs/tpcc-machines.txt "sudo pip3 install setuptools"
  ./run_command_on_all_hosts.sh -c ../tmp-configs/tpcc-machines.txt "sudo pip3 install paramiko psutil numpy"
  ```
* Ensure that there are no transient hook files
  * `./run_command_on_all_hosts.sh -c ../tmp-configs/tpcc-machines.txt "rm -rf /hdd1/hooks/*"`
* Ensure Brad owns all the files
  * `./run_command_on_all_hosts.sh -c ../tmp-configs/tpcc-machines.txt "sudo chown -R mtabebe /hdd1/"`

## Running an Experiment

Between (failed) runs, make sure that state is OK:
```
./run_command_on_all_hosts.sh -c ../tmp-configs/tpcc-machines.txt "ps aux | grep hook | grep -v ssh | grep -v grep | rev | awk '{print \$(NF - 1)}' | rev | xargs sudo kill -9"
./run_command_on_all_hosts.sh -c ../tmp-configs/tpcc-machines.txt "/hdd1/dyna-mast/horizondb/deployment/scripts/kill_all_horizondb.sh"
./run_command_on_all_hosts.sh -c ../tmp-configs/tpcc-machines.txt "rm -rf /hdd1/hooks/*"
```

# Useful Commands

* Change git branches:
```
./run_command_on_all_hosts.sh -c ../tmp-configs/tpcc-machines.txt "cd /hdd1/dyna-mast/oltpbench/; git reset --hard; git checkout tpcc-new-order; git pull origin tpcc-new-order"
 ```
* How to kill commands running on all hosts (change hooks to whatever)
```
./run_command_on_all_hosts.sh -c ../tmp-configs/tpcc-machines.txt "ps aux | grep hooks | grep -v ssh | grep -v grep | rev | awk '{print \$(NF - 1)}' | rev | xargs sudo kill -9"
```
* How to kill any horizondb
```
./run_command_on_all_hosts.sh -c ../tmp-configs/tpcc-machines.txt "/hdd1/dyna-mast/horizondb/deployment/scripts/kill_all_horizondb.sh"
```

