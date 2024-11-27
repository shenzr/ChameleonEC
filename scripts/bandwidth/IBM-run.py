import os
import sys
import csv
import time
import subprocess
import numpy as np

repair_requestor = "Slave5"
# repair_requestor = "Slave20"
ycsb_slaves = []
#ycsb_requestors = ["Slave10", "Slave11", "Slave12", "Slave13"]
ycsb_requestors = ["Slave10", "Slave11", "Slave12"]
# ycsb_requestors = ["Slave11"]
# ycsb_requestors = ["Slave8", "Slave9", "Slave10", "Slave11", "Slave12", "Slave13"]
node_set = ["Slave2", "Slave3", "Slave4", "Slave5", "Slave6", "Slave7", "Slave8", "Slave9", "Slave10", "Slave11", "Slave12", "Slave13", "Slave14", "Slave15", "Slave16", "Slave17", "Slave18", "Slave19", "Slave20"]

filepath = os.path.realpath(__file__)
bandwidth_dir = os.path.dirname(os.path.normpath(filepath))
script_dir = os.path.dirname(os.path.normpath(bandwidth_dir))
home_dir = os.path.dirname(os.path.normpath(script_dir))

slave_OutputPath = "/root/ggw"

ycsbHome = "/root/ycsb-0.17.0"
hbaseHome = "/root/hbase-2.3.6"
facebook_Path="~/trace/motivation"

onlyFore = "0"
onlyRepair = "1"
foreAndRepair = "2"

DEV="ens5"

def closeNethogs():
    for node in node_set:
        command = "ssh " + "root@"+ node + " \"killall nethogs\" "
        print(command)
        subprocess.Popen(['/bin/bash', '-c', command])
    for node in node_set:
        command = "ssh " + "root@"+ node + " \"scp " + slave_OutputPath + "/*.csv Master:~/ggw/output/\" "
        print(command)
        os.system(command)


# # ycsb only
def ycsbOnly():
    threadb = []
    for node in node_set:
        command = "ssh " + "root@"+ node + " \"" + "cd " + slave_OutputPath + "; rm *.csv; " + "nethogs " + DEV + " -t > " + slave_OutputPath + "/" + node + "_output_" + onlyFore + ".csv\" "
        print(command)
        subprocess.Popen(['/bin/bash', '-c', command]) 

    for r in ycsb_requestors:
        #command = "ssh " + "root@"+ r + " \""+ "cd " + facebook_Path + "; ./testMem"  + "\" "
        command = "ssh " + "root@"+ r + " \""+ "cd " + facebook_Path + "; ./motivation"  + "\" "
        print(command)
        threadb.append(subprocess.Popen(['/bin/bash', '-c', command]))
    #command =  ycsbHome + "/bin/ycsb load hbase20 -P " + ycsbHome + "/workloads/testworkload -cp " + hbaseHome + "/conf/ -threads 4"
    #print(command)
    #os.system(command)

    for t in threadb:
        t.wait()

    closeNethogs()

def ycsbAndRepair():
    threadsa = []
    # for node in node_set:
    #     command = "ssh " + "root@"+ node + " \"" + "cd " + slave_OutputPath + "; rm *.csv; " + "nethogs " + DEV + " -t > " + slave_OutputPath + "/" + node + "_output_" + foreAndRepair + ".csv\" "
    #     print(command)
    #     subprocess.Popen(['/bin/bash', '-c', command]) 

    for r in ycsb_requestors:
        #command = "ssh " + "root@"+ r + " \""+ "cd " + facebook_Path + "; ./testMem"  + "\" "
        command = "ssh " + "root@"+ r + " \""+ "cd " + facebook_Path + "; ./motivation"  + "\" "
        print(command)
        threadsa.append(subprocess.Popen(['/bin/bash', '-c', command]))

    
    # -- start repair cluster
    command = "cd " + home_dir + " && python3 scripts/bandwidth/run-BD-Monitor.py "
    print(command)
    os.system(command)

    # command = "cd " + bandwidth_dir + " && python3 cgroup-start.py"
    # command = "cd " + home_dir + " && python3 scripts/bandwidth/trickle-start.py "
    command = "cd " + home_dir + " && python3 scripts/start.py "
    print(command)
    os.system(command)

    # time.sleep(22)

    command = "ssh " + repair_requestor + " \" cd " + home_dir + " && ./ECClient \""
    print(command)
    os.system(command)

    for t in threadsa:
        t.wait()

    command = "cd " + home_dir + " && python3 scripts/stop.py "
    os.system(command)

    command = "cd " + home_dir + " && python3 scripts/bandwidth/kill-BD-Monitor.py "
    print(command)
    os.system(command)

    #closeNethogs()

def repairOnly():
    for node in node_set:
        command = "ssh " + "root@"+ node + " \"" + "cd " + slave_OutputPath + "; rm *.csv; " + "nethogs " + DEV + " -t > " + slave_OutputPath + "/" + node + "_output_" + onlyRepair + ".csv\" "
        print(command)
        subprocess.Popen(['/bin/bash', '-c', command]) 

    # -- start repair cluster
    command = "cd " + home_dir + " && python3 scripts/start.py "
    print(command)
    os.system(command)
    
    command = "ssh " + repair_requestor + " \" cd " + home_dir + " && ./ECClient \""
    print(command)
    os.system(command)

    command = "cd " + home_dir + " && python3 scripts/stop.py "
    os.system(command)

    # closeNethogs()

if __name__ == '__main__':    
    if(len(sys.argv)) < 2:
        print("argument not enough, need operation type")
        print("\toperation type: 0 -> fore only, 1 -> fore and repair, 2 -> repair only")
        sys.exit()
    op = int(sys.argv[1])

    if op!=0 and op!=1 and op!=2 and op!=3:
        print("argument error, need 0 or 1 or 2")
        sys.exit()

    if op == 0:
        ycsbOnly()
    elif op == 1:
        ycsbAndRepair()
    elif op ==2:
        repairOnly()
        
