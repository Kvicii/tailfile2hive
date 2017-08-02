#! /bin/sh 
set -x
if (($#!=0));then
    df=${1}
    df2=`date -d" ${df}" "+%Y-%m-%d"`
    df3=`date -d" ${df}" "+%Y_%m_%d"` 
else
    tr=`date "+%Y%m%d"`
    df=`date -d"1 day ago ${tr} " "+%Y%m%d"`
    df2=`date -d"1 day ago ${tr} " "+%Y-%m-%d"`
    df3=`date -d"1 day ago ${tr} " "+%Y_%m_%d"`
fi

python tail2hive.py   -l  /usr/local/nginx/logs/log.dreamlive.tv.accessnew.log -b  10000
