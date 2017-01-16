#!/bin/bash

export METRIC="Fp1"
export SID="S001"
export RID="R01"

hadoop jar target/eeg-hadoop-moving-average-1.0-SNAPSHOT-job.jar \
-Deeg.movingaverage.N=5 \
-Deeg.input.metric=$METRIC \
-Deeg.input.sid=$SID \
-Deeg.input.rid=$RID

hadoop jar target/eeg-hadoop-moving-average-1.0-SNAPSHOT-job.jar \
-Deeg.movingaverage.N=10 \
-Deeg.input.metric=$METRIC \
-Deeg.input.sid=$SID \
-Deeg.input.rid=$RID

hadoop jar target/eeg-hadoop-moving-average-1.0-SNAPSHOT-job.jar \
-Deeg.movingaverage.N=20 \
-Deeg.input.metric=$METRIC \
-Deeg.input.sid=$SID \
-Deeg.input.rid=$RID