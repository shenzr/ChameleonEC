import os
import subprocess

filepath = os.path.realpath(__file__)
init_dir = os.path.dirname(os.path.normpath(filepath))
script_dir = os.path.dirname(os.path.normpath(init_dir))
home_dir = os.path.dirname(os.path.normpath(script_dir))
conf_dir = home_dir+"/conf"
meta_dir = home_dir+"/meta"
CONF = conf_dir+"/config.xml"
# print(CONF)
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
print("start init")


for slave in slavelist:
    # cmd = "ssh " + slave + " \" cd ElasticRepair && mkdir logs \" "
    # cmd = "ssh " + slave + " \" mkdir ElasticRepair && mkdir ElasticRepair/conf && mkdir ElasticRepair/scripts && mkdir ElasticRepair/scripts/bandwidth && mkdir ElasticRepair/logs/\" "
    print(cmd)
    os.system(cmd)

