import os
import subprocess

filepath = os.path.realpath(__file__)
script_dir = os.path.dirname(os.path.normpath(filepath))
home_dir = os.path.dirname(os.path.normpath(script_dir))
conf_dir = home_dir+"/conf"
CONF = conf_dir+"/config.xml"
# print(CONF)

coor_ip = ""

def getHelpers(conf_path):

    # read from configuration file of the slaves
    f = open(conf_path)
    start = False
    concactstr = ""
    for line in f:
        if line.find("setting") == -1:
            line = line[:-1]
            concactstr += line
    res = concactstr.split("<attribute>")

    slavelist = []
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
        elif attr.find("coordinator.address") != -1:
            valuestart = attr.find("<value>")
            valueend = attr.find("</value>")
            attrtmp = attr[valuestart:valueend]
            coor_ip = str(attrtmp.split("<value>")[1])

    print(slavelist)
    return coor_ip, slavelist
 

def scp_config(ip, conf_path):
    command = "scp " + conf_path + " " + ip + ":" + conf_path
    print(command)
    os.system(command)

def edit_value(conf_path, key, value):
    f_conf = open(conf_path)
    concactstr = ""
    for line in f_conf:
        if line.find("setting") == -1:
            line = line[:-1]
            concactstr += line
    res = concactstr.split("<attribute>")

    for attr in res:
        if attr.find(key) != -1: # i.e., packet.size
            old_value = attr
            pos_end = attr.find("<value>") + len("<value>")
            pos_beg = attr.find("</value>")
            assert(pos_end < len(attr))
            assert(pos_beg < len(attr))
            new_value = attr[:pos_end] + value + attr[pos_beg:]
            command = "sed -i " + "\'s@" + old_value + "@" + new_value + "@g\' " + conf_path
            print("old value = " + old_value)
            print("new value = " + new_value)
            # print(command)
            os.system(command)
            break
    f_conf.close()


coor_ip, ips = getHelpers(CONF)

for i in range(len(ips)):
    ip = ips[i]
    edit_value(CONF, "local.ip.address", ip)
    scp_config(ip, CONF)

edit_value(CONF, "local.ip.address", coor_ip)
scp_config(coor_ip, CONF)

