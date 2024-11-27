import os
import csv
import sys
import time
import subprocess
import numpy as np
#import matplotlib.pyplot as plt

filepath = os.path.realpath(__file__)
script_dir = os.path.dirname(os.path.normpath(filepath))
home_dir = os.path.dirname(os.path.normpath(script_dir))

sys.stdout = open(home_dir + "/logs/repair-bd-log", "w")
repair = ":6379"
repairr = "ECHelper"
#ycsb = "/usr/lib/jvm/java-8-oracle/bin/java/"
LASTLINEID = -1

def get_bandwidth(recv, send):
    fore_ground_recv = 0
    fore_ground_send = 0
    for i in range(len(recv)):
        fore_ground_recv += float(recv[i])
        fore_ground_send += float(send[i])
        print('%.3f' % recv[i], '%.3f' % send[i])
    if len(recv) != 0:
        fore_ground_recv /= len(recv)
        fore_ground_send /= len(send)
        print("ave_recv = " '%.3f' % fore_ground_recv)
        print("ave_send = " '%.3f' % fore_ground_send)
    

def parse(data):
    #print("data.size = ", len(data))
    for i in range(len(data)):
        arr = str(data[i]).split("\\t")
        print(arr)
        #for j in range(len(arr)):
            #print(arr[j])


def target_to_refresh(file, T):
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
            if id > 0 and (str(all_rows[i]).find(repair) != -1 or str(all_rows[i]).find(repairr) != -1):
                s = str(all_rows[i]).split("\\t")[1]
                r = str(all_rows[i]).split("\\t")[2][:-2]
                recv[id-1] += float(r)
                send[id-1] += float(s)
                data.append(all_rows[i])
            #print(str(all_rows[i]))
    #parse(data)
    #print("")
    get_bandwidth(recv, send)


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
    
    cmd = "nethogs -t > " + file
    subprocess.Popen(['/bin/bash', '-c', cmd])
    time.sleep(1)
    while True:
        time.sleep(int(T))
        target_to_refresh(file, int(T))

