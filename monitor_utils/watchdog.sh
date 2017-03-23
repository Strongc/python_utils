#!/bin/bash
/root/service_monitor.sh mongos >> /root/watchdog.log
/root/service_monitor.sh mongod_configsvr >> /root/watchdog.log
