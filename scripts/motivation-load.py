import os
import sys
import subprocess

ips = ["172.31.0.202", "172.31.0.203", "172.31.0.204", "172.31.0.205", "172.31.0.206", "172.31.0.207", "172.31.0.208", "172.31.0.209", "172.31.0.210", "172.31.0.211", 
"172.31.0.212", "172.31.0.213"]

nodeSet = ["Slave2", "Slave3", "Slave4", "Slave5", "Slave6", "Slave7", "Slave8", "Slave9", "Slave10", "Slave11", "Slave12", "Slave13"]
ycsbSet = ["Slave10", "Slave11", "Slave12", "Slave13"]
failNode = "Slave3"

ycsbHome = "/root/ycsb-0.17.0"
hbaseHome = "/root/hbase-2.3.6"
slave_OutputPath = "/root/ggw"
repairboostPath = "/root/ElasticRepair"

onlyFore = "0"
onlyRepair = "1"
foreAndRepair = "2"

DEV="ens5"

def runForeAndRepair():
    command =  ycsbHome + "/bin/ycsb load hbase20 -P " + ycsbHome + "/workloads/testworkload -cp " + hbaseHome + "/conf/ -threads 4"
    print(command)
    subprocess.Popen(['/bin/bash', '-c', command])
    
    command = "python3 " + repairboostPath + "/scripts/start.py"
    print(command)
    os.system(command)

    command = command = "ssh " + "root@"+ failNode + " \"cd " + repairboostPath + "; ./ECClient\" "
    print(command)
    os.system(command)
    
    command = "python3 " + repairboostPath + "/scripts/stop.py"
    print(command)
    os.system(command)


if __name__ == '__main__':
    
    runForeAndRepair()
