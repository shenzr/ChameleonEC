// generates a random placement

#include <fstream>
#include <iostream>

#include "config.h"

extern "C" {
    #include "common.h"
}

using namespace std;

int main(int argc, char* argv[]) {
    if (argc != 6) {
        printf("ERR: ./gene_placement (ecK) (ecM) (stripe_num) (node_num) (rack_num)\n");
        printf("\t for LRC code, ecM = LRC_L + LRC_G\n");
        exit(1);
    }

    int ecK = atoi(argv[1]);
    int ecM = atoi(argv[2]);
    int stripe_num = atoi(argv[3]);
    int node_num = atoi(argv[4]);
    int rack_num = atoi(argv[5]);

    if (node_num % rack_num != 0) {
        printf("ERR: node_num mod rack_num!=0\n");
        exit(1);
    }
    printf("node_num=%d, rack_num=%d, stripe_num = %d\n", node_num, rack_num, stripe_num);

    int* placement = (int*)malloc(sizeof(int) * (ecK + ecM) * stripe_num);
    gene_rndm_plcement(ecK, ecM, placement, stripe_num, node_num, rack_num);

    // write the placement
    string path = "placements/placement";
    path = path + "_" + to_string(ecK) + "_" + to_string(ecK+ecM) + "_" + to_string(node_num) + "_" + to_string(stripe_num);
    ofstream output(path);
    int i, j;

    for (i = 0; i < stripe_num; i++) {
        for (j = 0; j < ecK + ecM; j++)
            output << placement[i * (ecK + ecM) + j] << " ";

        output << endl;
    }

    free(placement);
}
