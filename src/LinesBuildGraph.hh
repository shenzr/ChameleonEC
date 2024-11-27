#ifndef _LINES_BUILD_GRAPH_HH_
#define _LINES_BUILD_GRAPH_HH_

#include <set>
#include <cmath>
#include <vector>
#include <queue>
#include <unistd.h>
#include <assert.h>
#include <algorithm>

#include "Config.hh"
#include "LinesDefine.hh"
#include "LinesStripe.hh"

const double eps = 1e-6;
class LinesBuildGraph {
    protected:
        int slaveCnt;
        int eck;
        int ecn;
        int ecm;
        int ec_LRC_L;
        int ec_LRC_G;
        double periodT;
        Config* _conf;
        
    public:
        void initConf(Config*);
        vector<Stripe> buildFlexibleGraph(int&, int*, vector<int>&, vector<int>&, int, int*, int*, int*, map<int, int>&, int, vector<int>&); 
};

#endif //_LINES_BUILD_GRAPH_HH_