#!/bin/bash

export METRIC="Fp1"
export SID="S001"
export RID="R01"

hadoop jar target/eeg-hadoop-paa-sax-1.0-SNAPSHOT-job.jar \
-Deeg.paasax.N=5 \
-Deeg.paasax.L=1408 \
-Deeg.input.metric=$METRIC \
-Deeg.input.sid=$SID \
-Deeg.input.rid=$RID

hadoop jar target/eeg-hadoop-paa-sax-1.0-SNAPSHOT-job.jar \
-Deeg.paasax.N=10 \
-Deeg.paasax.L=1408 \
-Deeg.input.metric=$METRIC \
-Deeg.input.sid=$SID \
-Deeg.input.rid=$RID

hadoop jar target/eeg-hadoop-paa-sax-1.0-SNAPSHOT-job.jar \
-Deeg.paasax.N=20 \
-Deeg.paasax.L=1408 \
-Deeg.input.metric=$METRIC \
-Deeg.input.sid=$SID \
-Deeg.input.rid=$RID