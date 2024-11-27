#!/bin/bash
DEV="ens5"
tc class add dev $DEV parent 1: classid 1:100 htb rate 1000mbit ceil 1000mbit
tc filter add dev $DEV parent 1:0 protocol ip prio 1 handle 1:100 cgroup