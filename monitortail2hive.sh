#! /bin/sh 
set -x
if (($#!=0));then
    df=${1}
    df2=`date -d" ${df}" "+%Y-%m-%d"`
    df3=`date -d" ${df}" "+%Y_%m_%d"` 
else
    tr=`date "+%Y%m%d"`
    df=`date -d"0 day ago ${tr} " "+%Y%m%d"`
    df2=`date -d"0 day ago ${tr} " "+%Y-%m-%d"`
    df3=`date -d"0 day ago ${tr} " "+%Y_%m_%d"`
fi
set -x
ps -fe|grep tail2hive  |grep -v 'grep' | grep -v 'monitortail2hive'
if [ $? -ne 0 ]
then
ssh dreamtv01 "python /home/zsl/sendmailargs.py 'zhushunli@dreamlive.tv'  '159机器tail2hive.py进程挂掉,重新启动'  '159机器tail2hive脚本挂掉，重新启动'"
cd /data/zsl/tail2hive
ssh hadoop01 "hive -e  'alter table  orc_partition_realtime_mapnewaccesslog  drop partition(day=${df},processmachine=159);'"
nohup sh tail2hive.sh >./tail2hive.log 2>&1 &
echo "进程挂掉....."
else
echo "runing....."
fi



