import os
import csv
import sys
import time
import subprocess
import numpy as np
from redis import Redis

DEV="ens5"
TOTAL_BD = 1000
MIN_REPAIR_BD = 0.3*TOTAL_BD
# MIN_REPAIR_BD = 0.1*TOTAL_BD
alpha = 1

filepath = os.path.realpath(__file__)
bandwidth_dir = os.path.dirname(os.path.normpath(filepath))
script_dir = os.path.dirname(os.path.normpath(bandwidth_dir))
home_dir = os.path.dirname(os.path.normpath(script_dir))
conf_dir = home_dir+"/conf"
CONF = conf_dir+"/config.xml"
sys.stdout = open(home_dir + "/logs/bd-output", "w")
coor_ip = ""
local_ip = ""
ycsb = "java"
LASTLINEID = -1

def init():
    global coor_ip, local_ip
    # read from configuration file of the slaves
    f = open(CONF)
    start = False
    concactstr = ""
    for line in f:
        if line.find("setting") == -1:
            line = line[:-1]
            concactstr += line
    res = concactstr.split("<attribute>")

    slavelist = []
    for attr in res:
        if attr.find("local.ip.address") != -1:
            valuestart = attr.find("<value>")
            valueend = attr.find("</value>")
            attrtmp = attr[valuestart:valueend]
            local_ip = str(attrtmp.split("<value>")[1])
        elif attr.find("coordinator.address") != -1:
            valuestart = attr.find("<value>")
            valueend = attr.find("</value>")
            attrtmp = attr[valuestart:valueend]
            coor_ip = str(attrtmp.split("<value>")[1])
    local_ip = local_ip.split('.')[3]
    print(local_ip)
    print(coor_ip)

def limit_repair_bandwidth(repair_bd_recv, repair_bd_send):
    limit_bd = repair_bd_send
    cmd = "tc class change dev " + DEV + " parent 1: classid 1:100 htb rate " + str(limit_bd) + "mbit"
    os.system(cmd)
    print(cmd)
    

def get_bandwidth(recv, send, T, coor_connect):
    fore_ground_recv = 0
    fore_ground_send = 0
    for i in range(len(recv)):
        fore_ground_recv += float(recv[i])
        fore_ground_send += float(send[i])
        print('%.3f' % recv[i], '%.3f' % send[i])
    # convert KB/s -> mbit/s
    fore_ground_recv /= float(T) * 128.0
    fore_ground_send /= float(T) * 128.0
    print("ave_recv = " '%.3f' % fore_ground_recv)
    print("ave_send = " '%.3f' % fore_ground_send)
    repair_bd_recv = int(max(MIN_REPAIR_BD, alpha * TOTAL_BD - fore_ground_recv))
    repair_bd_send = int(max(MIN_REPAIR_BD, alpha * TOTAL_BD - fore_ground_send))
    coor_connect.set('recovery_up_' + local_ip, '%d'%repair_bd_send)
    coor_connect.set('recovery_dw_' + local_ip, '%d'%repair_bd_recv)
    #result = coor_connect.get('recovery_up')
    #print("up ", result, type(result))
    # limit_repair_bandwidth(repair_bd_recv, repair_bd_send)

def parse(data):
    #print("data.size = ", len(data))
    for i in range(len(data)):
        arr = str(data[i]).split("\\t")
        print(arr)
        #for j in range(len(arr)):
            #print(arr[j])


def target_to_refresh(file, T, coor_connect):
    global LASTLINEID
    recv = []
    send = []
    data = []
    with open(file) as f:
        reader = csv.reader(f)
        all_rows = list(reader)
        all_len = len(all_rows)
        id = 0
        #print("LastLinedId is ", LASTLINEID)
        for i in range(LASTLINEID+1, all_len):
            if all_rows[i] == []:
                continue
            if str(all_rows[i]) == "['Refreshing:']":
                id += 1
                if id > T:
                    LASTLINEID = i-1
                    break
                recv.append(0)
                send.append(0)
                continue
            if id > 0 and str(all_rows[i]).find(ycsb) != -1:
                s = str(all_rows[i]).split("\\t")[1]
                r = str(all_rows[i]).split("\\t")[2][:-2]
                recv[id-1] += float(r)
                send[id-1] += float(s)
                data.append(all_rows[i])
            #print(str(all_rows[i]))
    #parse(data)
    #print("")
    get_bandwidth(recv, send, T, coor_connect)


if __name__ == '__main__':   
    if(len(sys.argv)) < 3:
        print("argument not enough, need file_path, T")
        print("\tfile: the .csv file of nethogs")
        print("\tT: the length of the period")
        sys.exit()
    file, T = sys.argv[1:3]
    
    if(int(T) < 1):
        print("T must be > 0")
        sys.exit()

    init()

    coor_connect = Redis(host=coor_ip, port=6379, db=0)
    coor_connect.set('recovery_up_' + local_ip, '0')
    coor_connect.set('recovery_dw_' + local_ip, '0')
    #coor_connect.connection_pool.disconnect()
   
    
    cmd = "nethogs " + DEV + " -t > " + file
    subprocess.Popen(['/bin/bash', '-c', cmd])
    time.sleep(1)
    while True:
        time.sleep(int(T))
        target_to_refresh(file, int(T), coor_connect)