import os
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

def testForChangeBD(slave):
    # -- test for change bandwidth
    command = "ssh " + slave + " \" tc class change dev eno1 parent 1: classid 1:10 htb rate 100mbit\""
    print(command)
    subprocess.Popen(['/bin/bash', '-c', command])

def makeDir(slave):
    command = "ssh " + slave + " \" cd " + script_dir + " && mkdir bandwidth\" "
    print(command)
    os.system(command)

def installPacks(slave):
    command = "ssh " + slave + " pip3 install numpy"
    os.system(command)

def configEnvironmentValues(slave):
    command = "ssh " + slave +  " \" echo export YCSB_HOME=/home/sy/linesyao/ycsb-0.17.0 >> /root/.bashrc \""
    os.system(command)
    print(command)
    command = "ssh " + slave +  " \" echo export ELA=/home/sy/linesyao/ElasticRepair >> /root/.bashrc \""
    os.system(command)
    print(command)
    command = "ssh " + slave + " \" source /root/.bashrc \" "

def netcls(slave):
    command = "ssh " + slave + " \" cd /sys/fs/cgroup/net_cls && mkdir elastic-repair/ \" "
    print(command)
    os.system(command)
    command = "ssh " + slave + " \" cd /sys/fs/cgroup/net_cls && echo 0x10100 > elastic-repair/net_cls.classid\" "
    print(command)
    os.system(command)

def netttcls(slave):
    command = "ssh " + slave + " \" cd /sys/fs/cgroup/net_cls && mkdir something/ \" "
    print(command)
    os.system(command)
    command = "ssh " + slave + " \" cd /sys/fs/cgroup/net_cls && echo 0x11000 > something/net_cls.classid\" "
    print(command)
    os.system(command)

for slave in slavelist:
    netcls(slave)
    netttcls(slave)
    #os.system("scp /home/sy/linesyao/ElasticRepair/scripts/BandWidthMonitor.py " + slave + ":/home/sy/linesyao/ElasticRepair/scripts/")
