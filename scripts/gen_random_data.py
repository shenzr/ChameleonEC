import os
import sys
import subprocess
import random
from time import *

filepath = os.path.realpath(__file__)
script_dir = os.path.dirname(os.path.normpath(filepath))
home_dir = os.path.dirname(os.path.normpath(script_dir))
print(home_dir)

meta_dir = home_dir + "/meta"


def generate_data(block_size, ecK):
    size = int(block_size * ecK)
    
    begin_time = time()
    rand_str = ""
    with open(meta_dir + "/random_data.txt", "rb") as f:
        rand_str = f.read()

    # get random data
    rand_len = len(rand_str)
    start_pos = random.randint(0, rand_len-size)
    end_pos = start_pos+size
    stripe_str = rand_str[start_pos:end_pos]

    print(start_pos, end_pos, len(stripe_str))

    # write to input.txt
    with open(meta_dir + "/input.txt", "wb") as f:
        f.write(stripe_str)

    end_time = time()
    print("run_time = ", end_time-begin_time)

if __name__ == '__main__':
    if len(sys.argv) != 3:
        print("./gen_random_data (block_size) (ecK)")
        exit(0)
    block_size = int(sys.argv[1])
    ecK = int(sys.argv[2])
    generate_data(block_size, ecK)
