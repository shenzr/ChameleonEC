import os
import sys
import subprocess

filepath = os.path.realpath(__file__)
bandwidth_dir = os.path.dirname(os.path.normpath(filepath))
script_dir = os.path.dirname(os.path.normpath(bandwidth_dir))
home_dir = os.path.dirname(os.path.normpath(script_dir))
conf_dir = home_dir+"/conf"
meta_dir = home_dir+"/meta"
CONF = conf_dir+"/config.xml"
f = open(CONF)
start = False
concactstr = ""
for line in f:
    if line.find("setting") == -1:
        line = line[:-1]
        concactstr += line
res = concactstr.split("<attribute>")

slavelist = []
metaStripeDir = ""
for attr in res:
    if attr.find("helpers.address") != -1:
        valuestart = attr.find("<value>")
        valueend = attr.find("</attribute>")
        attrtmp = attr[valuestart:valueend]
        slavestmp = attrtmp.split("<value>")
        for slaveentry in slavestmp:
            if slaveentry.find("</value>") != -1:
                entrysplit = slaveentry.split("/")
                slaveentry = entrysplit[1]
                endpoint = slaveentry.find("</value>")
                slave = slaveentry[:endpoint]
                slavelist.append(slave)
    elif attr.find("meta.stripe.dir") != -1:
        valuestart = attr.find("<value>")
        valueend = attr.find("</value>")
        attrtmp = attr[valuestart:valueend]
        metaStripeDir = str(attrtmp.split("<value>")[1])

def testForChangeBD():
    # -- test for change bandwidth
    command = "ssh " + slave + " \" tc class change dev eno1 parent 1: classid 1:10 htb rate 100mbit\""
    print(command)
    subprocess.Popen(['/bin/bash', '-c', command])

def makeDir():
    command = "ssh " + slave + " \" cd " + script_dir + " && mkdir bandwidth\" "
    print(command)
    os.system(command)

def installPacks():
    command = "ssh " + slave + " pip3 install numpy"
    os.system(command)


if __name__ == '__main__':    
    if(len(sys.argv)) < 2:
        print("argument not enough, need operation type")
        print("\toperation type: 0 -> fore only, 1 -> fore and repair")
        sys.exit()
    op = int(sys.argv[1])

    if op!=0 and op!=1:
        print("argument error, need 0 or 1")
        sys.exit()
        
    command = "cd " + bandwidth_dir+ " && python3 run-TC.py"
    print(command)
    os.system(command)

    command = "cd " + bandwidth_dir+ " && python3 run-BD-Monitor.py "
    print(command)
    os.system(command)


    command = "cd " + bandwidth_dir+ " && python3 ycsb-run.py " + str(op)
    print(command)
    os.system(command)

    command = "cd " + bandwidth_dir+ " && python3 kill-BD-Monitor.py "
    print(command)
    os.system(command)
