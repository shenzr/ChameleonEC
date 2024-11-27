import os
import subprocess

filepath = os.path.realpath(__file__)
script_dir = os.path.dirname(os.path.normpath(filepath))
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

# start


for slave in slavelist:
    #command = "ssh " + slave +  " \" echo export YCSB_HOME=/home/sy/linesyao/ycsb-0.17.0 >> /root/.bashrc \""
    #os.system(command)
    #print(command)
    os.system("scp " + script_dir + "/repair-BD.py " + slave + ":" + script_dir + "/")
    
    command = "ssh " + slave + " \" python3 "+ script_dir + "/repair-BD.py " + home_dir + "/logs/repair-bd-output.csv 5 &> "+ home_dir + "/logs/repair-BD-interactive &\" "
    print(command)
    subprocess.Popen(['/bin/bash', '-c', command])
