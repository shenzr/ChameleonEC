#include "LinesTranScheduling.hh"

void unitTestForSchedulingScheme(vector<Stripe>& stripes) {
    int INF = 0x3f3f3f3f;
    int lostBlkCnt = stripes.size();
    // -- check if all edges are scheduled 
    // if(schedule_method == SCHEDULE_METHOD_BOOST) 
    for(int i=0; i<lostBlkCnt; ++i) {
        for(int j=0; j<stripes[i].rG._node_cnt; ++j) {
            if(stripes[i].rG.in[j][0] || stripes[i].rG.out[j][0]) {
                cout << "ERR: check wrong, isap not finish" << endl;
                exit(1);
            }
        }
    }
        
    // -- Check whether the repair sequence matches the topology
    // For each point in the stripe, all downloads need to be completed before upload
    int to, from;
    int latest_dw, earliest_up;
    for(int i=0; i<lostBlkCnt; ++i) {
        for(int j=0; j<stripes[i].rG_bp._node_cnt; ++j) {
            if(!stripes[i].rG_bp.out[j][0] || !stripes[i].rG_bp.in[j][0]) continue;  
            latest_dw = -1, earliest_up = INF;
            for(int x=1; x<=stripes[i].rG_bp.out[j][0]; ++x) {
                to = stripes[i].rG_bp.out[j][x];
                earliest_up = min(earliest_up, stripes[i].repairOrder[j][to]);
            }
            for(int x=1; x<=stripes[i].rG_bp.in[j][0]; ++x) {
                from = stripes[i].rG_bp.in[j][x];
                latest_dw = max(latest_dw, stripes[i].repairOrder[from][j]);
            }
            if(earliest_up <= latest_dw) ER5;

        }
    }
}


void LinesTranScheduling::initConf(Config* conf) {
    _conf = conf;
    _ecK = _conf->_ecK;
    _ecN = _conf->_ecN;
    _ecM = _ecN - _ecK;
    _ec_LRC_L = _conf->_lrcL;
    _ec_LRC_G = _ecM - _ec_LRC_L;
    _slaveCnt = _conf->_helpersIPs.size();

    _linkMap.clear();

    _node_in_count = (int*)calloc(_slaveCnt, sizeof(int));
    _node_out_count = (int*)calloc(_slaveCnt, sizeof(int));

    _exp_up_time = (double*)calloc(_slaveCnt, sizeof(double));
    _exp_dw_time = (double*)calloc(_slaveCnt, sizeof(double));
    cout << "finish initConf" << endl;
}

void add_count(int* arr, int node_id, priority_queue<pair<double, int> >& que, double exp_time) {
    if(!arr[node_id]) {
        que.push({exp_time, node_id});
    }
    ++arr[node_id];
}

void LinesTranScheduling::getEdgesWithoutDep(vector<Stripe>& stripes) {
    int sz = stripes.size();

    int from, to, rank;
    for(int i=0; i<sz; ++i) {
        for(int j=0; j<stripes[i].rG._node_cnt; ++j) {
            if(!stripes[i].rG.in[j][0] && stripes[i].rG.out[j][0]) { // indegree=0 and outdegree>0
                from = stripes[i].vertex_to_peerNode[j];
                to = stripes[i].vertex_to_peerNode[stripes[i].rG.out[j][1]];
                rank = -stripes[i].rG.in[stripes[i].rG.out[j][1]][0]; // prior for 可能激活新边
                //_linkMap[{from, to}].push({-rank, i});
                _linkMap[{from, to}].push({&(stripes[i].rG.in[stripes[i].rG.out[j][1]][0]), i});

                add_count(_node_in_count, to, _dwload_nodes, _exp_dw_time[to]);
                add_count(_node_out_count, from, _upload_nodes, _exp_up_time[from]);
            }
        }
    }
    cout << "finish getEdgesWithoutDep" << endl;
}

void LinesTranScheduling::doScheduling(vector<Stripe>& stripes, vector<int>& up_bd, vector<int>& dw_bd, int blkSize) {
    pii tmp_pii;
    double tmp_up_t, tmp_dw_t;
    vector<pii> tmp_up_nodes, tmp_dw_nodes;
    int vid_fr, vid_to, sid;
    int up_node_id, dw_node_id, order = 0;

    int cur = 0;

    // iteratively find the lightest up_node and dw_node
    while(!_upload_nodes.empty()) {
        tmp_up_nodes.clear();
        tmp_dw_nodes.clear();
        
        if(!_node_in_count[_dwload_nodes.top().second]) exit(-1);
        if(!_node_out_count[_upload_nodes.top().second]) exit(-1);

        // choose the link
        if(_upload_nodes.top().first > _dwload_nodes.top().first) { 
            // lightest upload
            up_node_id = _upload_nodes.top().second;
            while(!_dwload_nodes.empty()) {
                dw_node_id = _dwload_nodes.top().second;
                if(_linkMap[{up_node_id, dw_node_id}].size()) break; // find the usable edge
                tmp_dw_nodes.push_back({_dwload_nodes.top().first, dw_node_id});
                _dwload_nodes.pop();
            }
        } else {
            // lightest dwload
            dw_node_id = _dwload_nodes.top().second;
            while(!_upload_nodes.empty()) {
                up_node_id = _upload_nodes.top().second;
                if(_linkMap[{up_node_id, dw_node_id}].size()) break; // find the usable edge
                tmp_up_nodes.push_back({_upload_nodes.top().first, up_node_id});
                _upload_nodes.pop();
            }
        }

        // assign the repair task  up_node_id->dw_node_id
        sid = _linkMap[{up_node_id, dw_node_id}].top().second;
        _linkMap[{up_node_id, dw_node_id}].pop();
        vid_fr = stripes[sid].peer_to_vertex[up_node_id];
        vid_to = stripes[sid].peer_to_vertex[dw_node_id];
        stripes[sid].repairOrder[vid_fr][vid_to] = ++order;


        // upd DAG
        for(int x=1; x<=stripes[sid].rG.in[vid_to][0]; ++x) {
            if(stripes[sid].rG.in[vid_to][x] == vid_fr) {
                stripes[sid].rG.in[vid_to][x] = stripes[sid].rG.in[vid_to][stripes[sid].rG.in[vid_to][0]-1];
                break;
            }
        }
        --stripes[sid].rG.in[vid_to][0];
        for(int x=1; x<=stripes[sid].rG.out[vid_fr][0]; ++x) {
            if(stripes[sid].rG.out[vid_fr][x] == vid_to) {
                stripes[sid].rG.out[vid_fr][x] = stripes[sid].rG.out[vid_fr][stripes[sid].rG.out[vid_fr][0]-1];
                break;
            }
        }
        --stripes[sid].rG.out[vid_fr][0];
        
    
        // if node still has usable edge, push back to queue
        --_node_in_count[dw_node_id];
        --_node_out_count[up_node_id];
        tmp_up_t = _upload_nodes.top().first - 1.0 * blkSize / up_bd[up_node_id];
        tmp_dw_t = _dwload_nodes.top().first - 1.0 * blkSize / dw_bd[dw_node_id];
        _upload_nodes.pop(); _dwload_nodes.pop();
        if(_node_out_count[up_node_id]) {
            _upload_nodes.push({tmp_up_t, up_node_id});
        } else {
            _exp_up_time[up_node_id] = tmp_up_t;
        }

        if(_node_in_count[dw_node_id]) {
            _dwload_nodes.push({tmp_dw_t, dw_node_id});
        } else {
            _exp_dw_time[dw_node_id] = tmp_dw_t;
        }

        // add newly-usabled edges
        if(!stripes[sid].rG.in[vid_to][0] && stripes[sid].rG.out[vid_to][0]) {
            int from = dw_node_id;
            int to = stripes[sid].vertex_to_peer[stripes[sid].rG.out[vid_to][1]];
            //_linkMap[{from, to}].push({-stripes[sid].rG.in[stripes[sid].rG.out[vid_to][1]][0], sid});
            _linkMap[{from, to}].push({&(stripes[sid].rG.in[stripes[sid].rG.out[vid_to][1]][0]), sid});

            add_count(_node_in_count, to, _dwload_nodes, _exp_dw_time[to]);
            add_count(_node_out_count, from, _upload_nodes, _exp_up_time[from]);
        }
        
        
        for(auto up : tmp_up_nodes) _upload_nodes.push({up.first, up.second});
        for(auto dw : tmp_dw_nodes) _dwload_nodes.push({dw.first, dw.second});
        ++cur;
    }

    unitTestForSchedulingScheme(stripes);
    cout << cur << " finish the doScheduling" << endl;
}

void LinesTranScheduling::TransmissionScheduling(Config* conf, vector<Stripe>& stripes, vector<int>& up_bd, vector<int>& dw_bd, int blkSize) {
    initConf(conf);
    getEdgesWithoutDep(stripes);
    doScheduling(stripes, up_bd, dw_bd, blkSize);
}