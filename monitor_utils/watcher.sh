#!/bin/bash

/home/pengxuelin/tool/monitor.sh mongos 27017 2>&1 >>/home/pengxuelin/tool/monitor.report
/home/pengxuelin/tool/monitor.sh mongod 27018 2>&1 >>/home/pengxuelin/tool/monitor.report
/home/pengxuelin/tool/monitor.sh mongod 27019 2>&1 >>/home/pengxuelin/tool/monitor.report
/home/pengxuelin/tool/monitor.sh mongod 37019 2>&1 >>/home/pengxuelin/tool/monitor.report

