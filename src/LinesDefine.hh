#ifndef _LINES_DEFINE_HH_
#define _LINES_DEFINE_HH_

#define COEF_SELECT_STATUS 1001

#define CLIENT_DEGRADED_READ 0
#define CLIENT_FULLNODE_RECOVERY 1

#define SCHEDULE_METHOD_ONLINE 0
#define SCHEDULE_METHOD_BOOST 1
#define SCHEDULE_METHOD_RANDOM 2
#define SCHEDULE_METHOD_LRU 3

#define SINGLE_STRIPE_CR 0
#define SINGLE_STRIPE_PPR 1
#define SINGLE_STRIPE_PATH 2

// error messages
#define ER0 {cout << "Correctness test fail!" << endl << "ER0:vertex map to -1" << endl; exit(1);}
#define ER1 {cout << "Correctness test fail!" << endl << "ER1:vertex map to fail_node" << endl; exit(1);}
#define ER2 {cout << "Correctness test fail!" << endl << "ER2:vertex map to wrong replacement node" << endl; exit(1);}
#define ER3 {cout << "Correctness test fail!" << endl << "ER3:vertex map to wrong source node" << endl; exit(1);}
#define ER4 {cout << "Correctness test fail!" << endl << "ER4:different vertexs map to the same peer node" << endl; exit(1);}
#define ER5 {cout << "Correctness test fail!" << endl << "ER5:Topology not satisfied" << endl; exit(1);}
#define ER6 {cout << "Correctness test fail!" << endl << "ER6:peer_node send/receive more than one chunk in the round" << endl; exit(1);}
#define ER7 {cout << "Correctness test fail!" << endl << "ER7:occupy the same round more than once" << endl; exit(1);}

#include <string>
using namespace std;

string ip2Str(unsigned int);


#endif

