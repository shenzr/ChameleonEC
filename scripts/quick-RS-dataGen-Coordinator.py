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
CONF = conf_dir + "/config.xml"
standalone_test_dir = meta_dir + "/standalone-blocks"
stripe_store_dir = meta_dir + "/standalone-meta"
print(CONF)

def solve(slave_ips, eck, ecn, node_num, block_size, stripe_num):
    # -- mkdir standalone-test & stripe-store
    for slave in slave_ips:
        os.system("ssh " + slave + " \"rm -r  " + standalone_test_dir +" \"")
        os.system("ssh " + slave + " \" mkdir  " + standalone_test_dir + " \"")
        os.system("scp " + script_dir + "/quick-RS-dataGen-Helper.py " + slave + ":" + script_dir)
    os.system(" rm -r  " + stripe_store_dir)
    os.system(" mkdir " + stripe_store_dir)

    # -- for the first time to generate a stripe
    command = "dd if=/dev/urandom iflag=fullblock of=" + meta_dir + "/random_data.txt bs=" + str(block_size) + " count=" +str(eck) + "; "
    os.system(command)
    os.system("python3 " + script_dir + "/gen_random_data.py " + str(block_size) + " " + str(eck))
    command = "cd " + test_dir + "; "
    command += "./createdata_rs ../conf/rsEncMat_" + str(eck)+ "_" + str(ecn)
    command += " ../meta/input.txt " + str(eck)+ " " + str(ecn) +"; "
    os.system(command)

    # -- for the first time to scp a stripe [file_k1 ... file_kk file_m1 ... file_mm] to all helpers, and send placement to all helpers
    for slave in slave_ips:
        for i in range(eck):
            command = "scp " + test_dir + "/file_k" + str(i+1) + " " + slave + ":" + meta_dir + "/"
            os.system(command)
        for i in range(ecn-eck):
            command = "scp " + test_dir + "/file_m" + str(i+1) + " " + slave + ":" + meta_dir + "/"
            os.system(command)

    # # -- generate a placement 
    command = "cd " + gene_place_dir + ";" + "make clean; make; ./gene_placement " + str(eck) + " " + str(ecn-eck) + " " + str(1000) + " " + str(node_num) + " " + str(node_num) + ";"
    print(command)
    os.system(command)
    print("gene_placement finish!")
    placement_file_path = gene_place_dir + "/placements/placement_" + str(eck) + "_" + str(ecn) + "_" + str(node_num) + "_" + str(1000)

    # # -- send placement to all helpers
    for slave in slave_ips:
        # command = "scp " + placement_file_path + " " + slave + ":" + gene_place_dir + "/placements/"
        command = "scp -r " + gene_place_dir + "/placements/ " + slave + ":" + gene_place_dir
        os.system(command)


    # -- store metadata info in coordinator
    blk_cnt = 0
    blk_node_ids = [[0 for i in range(int(ecn))] for j in range(int(stripe_num))] 
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

    for i in range(stripe_num):
        metaInfo = ""
        stripe_str_m = "stripe_" + str(i) + "_file_m"
        stripe_str_k = "stripe_" + str(i) + "_file_k"
        for j in range(eck):
            if j != 0:
                metaInfo += ":"
            metaInfo += stripe_str_k + str(j+1) + "_1001"
        for j in range(ecn-eck):
            metaInfo += ":"
            metaInfo += stripe_str_m + str(j+1) + "_1002"
        # print(metaInfo)
        # wirte meta info
        for j in range(ecn):
            if j < eck:
                filepath = stripe_store_dir + "/rs:" + stripe_str_k + str(j+1) + "_1001"
            else:
                filepath = stripe_store_dir + "/rs:" + stripe_str_m + str(j-eck+1) + "_1002"
            # print(filepath)
            f = open(filepath, "w+")
            f.write(metaInfo)
            f.close()

    # start quick-RS-dataGen-Helper in all helpers
    for slave in slave_ips:
        command = "ssh " + slave + " \" cd " + script_dir + " && python3 quick-RS-dataGen-Helper.py \" "
        # os.system(command)
        subprocess.Popen(['/bin/bash', '-c', command])
        




# read from configuration file of the slaves
_ecK = 0
_ecN = 0
_pktCnt = 0
_pktSize = 0

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
    elif attr.find("packet.size") != -1:
        valuestart = attr.find("<value>")
        valueend = attr.find("</value>")
        attrtmp = attr[valuestart:valueend]
        _pktSize = int(attrtmp.split("<value>")[1]) 
    elif attr.find("packet.count") != -1:
        valuestart = attr.find("<value>")
        valueend = attr.find("</value>")
        attrtmp = attr[valuestart:valueend]
        _pktCnt = int(attrtmp.split("<value>")[1])

print("start Lines quick data gen!")
node_num = int(len(slavelist))
stripe_num = 10
print("ecK = " + str(_ecK) + ", ecN = " + str(_ecN) + ", node_num = " + str(node_num))

solve(slavelist, _ecK, _ecN, node_num, _pktCnt*_pktSize, stripe_num)





