export METRIC="Fp1"
export SID="S001"
export RID="R01"

curl "http://centos-master:4242/q?start=2009/08/14-19:15:51.402&end=2009/08/14-19:15:51.668&m=sum:$METRIC\{sid=$SID,rid=$RID\}&ascii&exact" -o $METRIC"_"$SID"_"$RID.txt
curl "http://centos-master:4242/q?start=2009/08/14-19:15:51.402&end=2009/08/14-19:15:51.668&m=sum:$METRIC\{sid=$SID,rid=$RID\}&json&exact" -o $METRIC"_"$SID"_"$RID.json