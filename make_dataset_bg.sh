#!/bin/zsh

# Download the dataset
mkdir -p logs
LOGFILE="logs/make_dataset_$(date +'%Y%m%d_%H%M%S').log"
nohup python3 -u src/make_dataset.py > $LOGFILE 2>&1 &
