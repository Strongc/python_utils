#! /bin/bash
# -*- coding: utf-8 -*-
if [ $# -lt 2 ]
then
    echo $#
    echo 'Usag : ./$0 <process_name> <port>'
    exit
fi

PATH=$PATH:/sbin/:/usr/sbin
cmd=$(which ifconfig)
process=$1
port=$2
process_count=$(ps -ef|grep $process|grep -v $0|grep -v grep|wc -l)
port_count=$(netstat -an|grep $port|wc -l)
echo "path=$PATH"
echo "ifconfig=$cmd"
echo $(ifconfig eth1|sed -n 's/inet [addr:]*\([0-9\.]*\).*/\1/gp'|sed 's/^[ \t]*//g')
ips=$(ifconfig eth1|sed -n 's/inet [addr:]*\([0-9\.]*\).*/\1/gp'|sed 's/^[ \t]*//g')

echo "process=$process, port=$port"
echo "process_count=$process_count"
echo "port_count=$port_count"
echo "ips=$ips"

if [ $process_count -lt 1 ] || [ $port_count -lt 1 ]
then
    echo "$(date +'%Y%m%dT%H:%M:%S') process $process:$port is not running! Please login onto server@$(hostname) $ips to check!" 
    echo "process $process:$port is not running! Please login onto server@$(hostname) with $ips to check!" | \
        #mail -s "process $process:$port is down!" pengxuelin@benditoutiao.com
        mail -s "process $process:$port is down!" pengxuelin@benditoutiao.com xujian@benditoutiao.com guxiangyu@benditoutiao.com
    echo ""
    exit 1
fi

echo ""

