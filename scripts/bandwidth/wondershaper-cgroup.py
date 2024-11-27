import os
import sys
import subprocess

ips = ["172.31.0.202", "172.31.0.203", "172.31.0.204", "172.31.0.205", "172.31.0.206", "172.31.0.207", "172.31.0.208", "172.31.0.209", "172.31.0.210", "172.31.0.211", 
"172.31.0.212", "172.31.0.213", "172.31.0.214", "172.31.0.215", "172.31.0.216", "172.31.0.217", "172.31.0.218", 
"172.31.0.219", "172.31.0.220"]

DEV="ens5"
cgroup_sh_path = "/root/ElasticRepair/scripts/bandwidth/"
cgroup_sh_name = "cgroup.sh"

def installwondershaper():
    for node in ips:
        command = "scp -r ~/wondershaper/ " + node + ":~/"
        print(command)
        os.system(command)

        command = "ssh -t " + node + " \" cd wondershaper && sudo make install\""
        print(command)
        os.system(command)

def initcgroup():
    for node in ips:
        command = "scp " + cgroup_sh_path + cgroup_sh_name + " " + node + ":" + cgroup_sh_path
        print(command)
        os.system(command)

        command = "ssh " + node + " \" cd " + cgroup_sh_path + " && chmod +x " + cgroup_sh_name + "\""
        print(command)
        os.system(command)

def startwondershaper():
    print("start")
    for node in ips:
        # command = "ssh " + node + " \" wondershaper -a " + DEV + " -d 2097152 -u 2097152 \" "
        command = "ssh " + node + " \" wondershaper -a " + DEV + " -d 1048576 -u 1048576 \" "
        # command = "ssh " + node + " \" wondershaper -a " + DEV + " -d 5242880 -u 5242880 \" "
        print(command)
        os.system(command)
        
        # command = "ssh " + node + " \" cd " + cgroup_sh_path + " && ./" + cgroup_sh_name + " \" "
        # print(command)
        # os.system(command)

def stopwondershaper():
    print("stop")
    for node in ips:
        command = "ssh " + node + " \"" +"wondershaper -c -a  " + DEV + " \""
        print(command)
        subprocess.Popen(['/bin/bash', '-c', command])
    

if __name__ == '__main__':
    if (len(sys.argv)) < 2:
        print("argument not enough:")
        print("\ttype: 0 -> start wondershaper; 1 -> stop wondershaper;")
        sys.exit()

    t = int(sys.argv[1])

    if t == 0:
        # initcgroup()
        startwondershaper()
    elif t == 1:
        stopwondershaper()
