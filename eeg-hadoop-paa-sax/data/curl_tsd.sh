export METRIC="Fp1"
export SID="S001"
export RID="R01"
export START_TIME="2009/08/14-19:14:00.000"
export END_TIME="2009/08/14-19:20:00.000"

echo "http://centos-master:4242/q?start="$START_TIME"&end="$END_TIME"&m=sum:$METRIC\{sid=$SID,rid=$RID\}&ascii&exact"
echo "http://centos-master:4242/q?start="$START_TIME"&end="$END_TIME"&m=sum:$METRIC\{sid=$SID,rid=$RID\}&json&exact"
curl "http://centos-master:4242/q?start="$START_TIME"&end="$END_TIME"&m=sum:$METRIC\{sid=$SID,rid=$RID\}&ascii&exact" -o $METRIC"_"$SID"_"$RID.txt
curl "http://centos-master:4242/q?start="$START_TIME"&end="$END_TIME"&m=sum:$METRIC\{sid=$SID,rid=$RID\}&json&exact" -o $METRIC"_"$SID"_"$RID.json
echo "Files are "$METRIC"_"$SID"_"$RID.txt" and "$METRIC"_"$SID"_"$RID.json

hadoop fs -rmr eeg-data
hadoop fs -mkdir eeg-data
hadoop fs -put $METRIC"_"$SID"_"$RID.txt eeg-data/$METRIC"_"$SID"_"$RID.txt