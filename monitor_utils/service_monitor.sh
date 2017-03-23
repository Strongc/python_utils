#!/bin/bash
PATH=$PATH:/sbin/:/usr/sbin
ips=$(ifconfig eth1|sed -n 's/inet [addr:]*\([0-9\.]*\).*/\1/gp'|sed 's/^[ \t]*//g')
name=$1
state=$(service $name status 2>/dev/null|grep Active | awk '{print $2}')
if [ "$state" != "active" ]
then
    echo "$(date +'%Y%m%dT%H:%M:%S') service $name is dead! Please login onto server@$(hostname) $ips to check!" 
    echo "service $name will be re-started automatically"
    echo "service $name is dead! Please login onto server@$(hostname) $ips to check!\r\n service will be re-started automatically" | \
	mail -s "service $name is dead!" pengxuelin@benditoutiao.com xujian@benditoutiao.com guxiangyu@benditoutiao.com
    service $name restart
fi

