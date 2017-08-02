#!/bin/bash
set -x
scp -r /data/zsl/tail2hive/newaccessmaphive.txt hadoop01:/data/zsl/realtime/159

while true
do
        process=`ssh hadoop01 "ps aux | grep realtime_mapnewaccesslog | grep -v grep"`
  
        if [ "$process" == "" ]; then
                echo "no process";
                ssh hadoop01 "hive -e  'load data local inpath \"/data/zsl/realtime/159/newaccessmaphive.txt\" overwrite  into table  map_159_realtime_mapnewaccesslog'"
                break;
        else
                sleep 2;
                echo "process exsits";              
        fi
done
while true
do
        process=`ssh hadoop01 "ps aux | grep realtime_mapnewaccesslog | grep -v grep"`
  
        if [ "$process" == "" ]; then
                echo "no process";
                ssh hadoop01 'hive -e "set hive.exec.dynamic.partition.mode=nonstrict; SET hive.exec.max.dynamic.partitions=100000;SET hive.exec.max.dynamic.partitions.pernode=100000; insert into table orc_partition_realtime_mapnewaccesslog partition(day,hour,processmachine) select linenum,time ,ip ,method ,request ,status ,body_bytes_sent ,http_referer ,useragent ,browser ,browerfamily ,browerversion ,os ,osfamily ,osversion ,device ,devicefamily ,devicebrand ,devicemodel ,request_time ,request_body ,http_x_forwarded_for ,act,loginfo,  substr(time,1,8) as day,substr(time,9,2) as hour ,'159' as  processmachine   from map_159_realtime_mapnewaccesslog;"'
                break;
        else
                sleep 2;
                echo "process exsits";              
        fi
done
wait
