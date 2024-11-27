#ifndef _LINES_TRAN_SCHEDULING_HH
#define _LINES_TRAN_SCHEDULING_HH

#include <set>
#include <map>
#include <queue>
#include <vector>
#include "Config.hh"
#include "LinesStripe.hh"

struct cmp{
    bool operator ()(const pair<int*, int> &a, const pair<int*, int> &b) {
        return *(a.first) < *(b.first);
    }
};

class LinesTranScheduling {
    protected:
        int _slaveCnt;
        int _ecK;
        int _ecN;
        int _ecM;
        int _ec_LRC_L;
        int _ec_LRC_G;
        Config* _conf;
    
        // priority_queue  (priority, stripe_id)
        //map<pii, priority_queue<pii> > _linkMap;
        map<pii, priority_queue<pair<int*, int>, vector<pair<int*, int> >, cmp> > _linkMap;

        // node作为可行边的出点的count
        int* _node_out_count;
        // node作为可行边的入点的count
        int* _node_in_count;

        // (-tran_up_time, node_id)
        priority_queue<pair<double, int> > _upload_nodes;
        priority_queue<pair<double, int> > _dwload_nodes;

        double* _exp_up_time;
        double* _exp_dw_time;

    public:
        void initConf(Config*);
        void TransmissionScheduling(Config*, vector<Stripe>&, vector<int>&, vector<int>&, int);
        void doScheduling(vector<Stripe>&, vector<int>&, vector<int>&, int);
        void getEdgesWithoutDep(vector<Stripe>& stripes);
};

#endif
