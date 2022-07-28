#!/bin/bash

while [ true ]; do 
    ps aux | grep oltp | grep -v grep | grep -v bash
    if [ $? -eq 0 ]; then
        echo "Waiting on OLTPBench..."
    fi
    sleep 10
done
