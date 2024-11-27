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

periodLen = 0
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
    elif attr.find("period.len") != -1:
        valuestart = attr.find("<value>")
        valueend = attr.find("</value>")
        attrtmp = attr[valuestart:valueend]
        periodLen = int(attrtmp.split("<value>")[1])

# start


for slave in slavelist:
    #os.system("scp " + script_dir + "/script-tc.sh " + slave + ":" + script_dir + "/")
    #os.system("ssh " + slave + " \" cd " + script_dir + " && chmod +x script-tc.sh\"")
    command = "ssh " + slave + " \" cd " + script_dir + " && sudo ./script-tc.sh \"" 
    print(command)
    os.system(command)
    #subprocess.Popen(['/bin/bash', '-c', command])
