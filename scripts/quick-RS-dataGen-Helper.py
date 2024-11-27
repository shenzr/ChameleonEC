import os
import subprocess

filepath = os.path.realpath(__file__)
script_dir = os.path.dirname(os.path.normpath(filepath))
home_dir = os.path.dirname(os.path.normpath(script_dir)) 
print(home_dir)

conf_dir = home_dir + "/conf"
test_dir = home_dir + "/test"
gene_place_dir = test_dir + "/gene_placement"
meta_dir = home_dir + "/meta"
standalone_test_dir = meta_dir + "/standalone-blocks/"
CONF = conf_dir + "/config.xml"
print(CONF)

local_ip = ""

def dataPlace(placement_file_path, eck, ecn, stripe_num, slave_id):
    blk_cnt = 0
    blk_node_ids = [[0 for i in range(int(ecn))] for j in range(int(stripe_num))] 
    # -- read placement info from placement_file
    with open(placement_file_path, "r") as ifs:
        for line in ifs:
            arr = line.split()
            # print(len(arr))
            if len(arr) != ecn:
                print("error len")
                os._exit(-1)
            for i in range(len(arr)):
                blk_node_ids[blk_cnt][i] = int(arr[i])
            blk_cnt += 1
            if blk_cnt >= stripe_num:
                break
    
    # -- place data
    for i in range(stripe_num):
        stripe_str_m = "stripe_" + str(i) + "_file_m"
        stripe_str_k = "stripe_" + str(i) + "_file_k"
        for j in range(eck):
            if blk_node_ids[i][j] == slave_id:
                command = "cp " + meta_dir + "/file_k" + str(j+1) + " " + standalone_test_dir + stripe_str_k + str(j+1)
                os.system(command)
        for j in range(ecn-eck):
            if blk_node_ids[i][j+eck] == slave_id:
                command = "cp " + meta_dir + "/file_m" + str(j+1) + " " + standalone_test_dir + stripe_str_m + str(j+1)
                os.system(command)


# read from configuration file of the slaves
_ecK = 0
_ecN = 0

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
    elif attr.find("erasure.code.k") != -1:
        valuestart = attr.find("<value>")
        valueend = attr.find("</value>")
        attrtmp = attr[valuestart:valueend]
        _ecK = int(attrtmp.split("<value>")[1])
    elif attr.find("erasure.code.n") != -1:
        valuestart = attr.find("<value>")
        valueend = attr.find("</value>")
        attrtmp = attr[valuestart:valueend]
        _ecN = int(attrtmp.split("<value>")[1]) 
    elif attr.find("local.ip.address") != -1:
        valuestart = attr.find("<value>")
        valueend = attr.find("</value>")
        attrtmp = attr[valuestart:valueend]
        local_ip = str(attrtmp.split("<value>")[1]) 
   
print("start Lines data gen in helper!")

stripe_num = 10
placement_file_path = gene_place_dir + "/placements/placement_" + str(_ecK) + "_" + str(_ecN) + "_" + str(len(slavelist)) + "_" + str(1000)
slave_id = -1
for i in range(len(slavelist)):
    if slavelist[i] == local_ip:
        slave_id = i
print("slave_id: " + str(slave_id))

dataPlace(placement_file_path, _ecK, _ecN, stripe_num, slave_id)
print("slave ip: " + local_ip + "finish dataPlace~~~~~~~~")




