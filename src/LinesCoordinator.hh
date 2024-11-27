#ifndef _LINES_COORDINATOR_HH_
#define _LINES_COORDINATOR_HH_

#include <cstring>
#include <algorithm>

#include "Coordinator.hh"
#include "LinesBuildGraph.hh"
#include "LinesTranScheduling.hh"
#include "LinesDealScheduling.hh"

using namespace std;

class LinesCoordinator : public Coordinator {

    map<int, unsigned int> _helperId2Ip;
    map<unsigned int, int> _helperIp2Id;

    // override
    void requestHandler();

   public:
    LinesCoordinator(Config* c);
    void parseDRRequest(char*, int, unsigned int&, string&, vector<pair<string, unsigned int>>&);
    void parseFNRequest(char*, int, vector<unsigned int>&, vector<string>&, int multiNodeFlag); 
    void getSlaveBDs(vector<int>&, vector<int>&, vector<string>);
    void sendCMDs(vector<pair<string, unsigned int> >, vector<Stripe>, vector<string>, int, redisContext*, unsigned int, vector<int>&, int);

    void getSurviHelperIds(map<string, int>*, vector<pair<string, int> >&);
    void solveFullNodeRequest(char*, int, redisContext*, int multiNodeFlag);
};

#endif  //_LINES_COORDINATOR_HH_
