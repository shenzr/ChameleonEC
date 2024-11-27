#include "LinesBuildGraph.hh"

void LinesBuildGraph::initConf(Config* conf) {
    _conf = conf;
    eck = _conf->_ecK;
    ecn = _conf->_ecN;
    ecm = ecn - eck;
    ec_LRC_L = _conf->_lrcL;
    ec_LRC_G = ecm - ec_LRC_L;
    slaveCnt = _conf->_helpersIPs.size();
    periodT = (double)_conf->_periodLen;
}

void unitTestForMapping(vector<Stripe>& stripes, int* placement, map<int, int>& isNodeIdFail, int slaveCnt, int ecN, vector<int>& batch_stripe_id) {
    int p, sid;
    int blkCnt = stripes.size();
    int* is_place = (int*)malloc(sizeof(int)*slaveCnt);
    for(int i=0; i<blkCnt; ++i) {
        memset(is_place, 0, sizeof(int)*slaveCnt);
        sid = batch_stripe_id[i];
        for(int j=0; j<ecN; ++j) {
            is_place[placement[sid*ecN+j]] = 1; 
        }
        for(int j=0; j<stripes[i].rG_bp._node_cnt; ++j) {
            p = stripes[i].vertex_to_peer[j];
            if(p == -1) ER0;
            // if(p == failNodeId) ER1; (upd 22.5.3)
            if(isNodeIdFail[p]) ER1;
            // j is the replacement node
            if(j == stripes[i].rG._node_cnt-1) {  
                if(is_place[p]) ER2;
                continue;
            }
            // j is the source node
            if(!is_place[p]) ER3; 
            if(is_place[p] == -1) ER4;
            is_place[p] = -1;
        }
    }
}

void displayDAG(int k, vector<vector<int> >& edges) {
    cout << "displayDAG:" << endl;
    for(int i=0; i<k; ++i) {
        for(int j=0; j<edges[i].size(); ++j) {
            cout << "node " << i << " -> node " << edges[i][j] << endl;
        }
    }
}

bool checkDAG(int k, vector<int>& indeg, vector<int>& outdeg, vector<vector<int> >& edges) {
    for(int i=0; i<=k; ++i) {
        if(indeg[i] || outdeg[i]) return false;
    }

    vector<int> visit(k+1, 0);
    for(int i=0, color=0; i<k; ++i) {
        if(!visit[i]) { // dfs
            visit[i] = ++color;
            for(int j=0; j<edges[i].size(); ++j) {
                if(visit[edges[i][j]] == color) return false; // cycle
                visit[edges[i][j]] = color;
            }
        }
    }

    return true;
}

bool checkDegree(int k, vector<int>& indeg, vector<int>& outdeg) {
    int insum = 0, outsum = 0;
    if(outdeg[k]) return false; // dest only download
    for(int i=0; i<k; ++i) {
        if(outdeg[i] != 1) return false; // source only send once
        insum += indeg[i];
        outsum +=outdeg[i];
    }
    insum += indeg[k], outsum += outdeg[k];
    return insum == k && outsum == k;
}


vector<vector<int> > constructDAG(int k, vector<int> indeg, vector<int> outdeg) {
    if(!checkDegree(k, indeg, outdeg)) exit(-1);
    vector<vector<int> > edges(k, vector<int>());
    if(indeg[k] == k) { // conventional repair
        for(int i=0; i<k; ++i) edges[i].push_back(k);
        return edges;
    }
    vector<int> special_up;
    vector<pair<int, int> > special_dw;
    set<int> nodes;
    for(int i=0; i<k; ++i) nodes.insert(i); 
    while(1) {
        if(nodes.size() == indeg[k]) break;
        special_up.clear();
        special_dw.clear();
        for(int i=0; i<k; ++i) {
            if(outdeg[i]>0 && !indeg[i]) special_up.push_back(i);
            if(indeg[i]>0) special_dw.push_back({indeg[i], i});
        }

        sort(special_dw.begin(), special_dw.end());
        int cnt_up = special_up.size();
        int cnt_dw = special_dw.size();

        for(int i=0; i<min(cnt_up, cnt_dw); ++i) {
            edges[special_up[i]].push_back(special_dw[i].second);
            --indeg[special_dw[i].second];
            --outdeg[special_up[i]];
            nodes.erase(special_up[i]);
        }
    }

    for(auto it=nodes.begin(); it!=nodes.end(); ++it) {
        edges[*it].push_back(k);
        --outdeg[*it];
        --indeg[k];
    }

    checkDAG(k, indeg, outdeg, edges);

    // displayDAG(k, edges);
    return edges;
}
/*
vector<Stripe> LinesBuildGraph::buildFlexibleGraph(int& remain, int* stripe_vis, vector<int>& up_bd, vector<int>& dw_bd, int lostBlkCnt, int* placement, int* isSoureCandidate, int* idxs, map<int, int>& isNodeIdFail, int blkSize, vector<int>& batch_stripe_id) {
    // for stripes, choose sources and dest for each stripe
    // up_bd, dw_bd for each storage node
    // expected_up_time, expected_dw_time for each storage node
    vector<Stripe> stripes;

    assert(up_bd.size() == slaveCnt && dw_bd.size() == slaveCnt);

    vector<double> expected_up_time(slaveCnt, 0), expected_dw_time(slaveCnt, 0), backup_dw(slaveCnt);

    int pid, destId;
    vector<int> vertexIds(eck+1), indegree(eck+1), outdegree(eck+1, 1);
    outdegree[eck] = 0;

    vector<int> sumindeg(slaveCnt, 0), sumoutdeg(slaveCnt, 0);

    vector<pair<double, int> > tmp_up(ecn-1), tmp_dw;
    int* vis = (int*)calloc(slaveCnt, sizeof(int));
    for(int i=0; i<lostBlkCnt; ++i) {
        int withdraw_flag = 0;
        if(stripe_vis[i]) continue;
        for(int j=0; j<slaveCnt; ++j) vis[j] = 0;
        // if LRC, judege whether local or global
        // choose sourceNum sources (eck for RS codes, eck or eck/ec_LRC_L for LRC codes)
        int sourceNum = eck;
        if(_conf->_ecType == "LRC" && idxs[i] < eck+ec_LRC_L) {
            // local
            sourceNum = ec_LRC_L ? eck/ec_LRC_L : 0;
            outdegree[sourceNum] = 0;
        }
     
        for(int j=0, ct=0; j<ecn; ++j) {
            pid = placement[i * ecn + j];
            vis[pid] = 1;
            // if(pid == failNodeId) continue; (upd 22.5.3) 
            if(isNodeIdFail[pid]) continue;
            if(!isSoureCandidate[i * ecn + j]) continue;
            tmp_up[ct++] = {expected_up_time[pid] + 1.0 * blkSize / up_bd[pid], pid};
            // prepare for withdraw
            backup_dw[pid] = expected_dw_time[pid];
        }
        sort(tmp_up.begin(), tmp_up.begin()+sourceNum);
        // -- check if time > periodT
        if(tmp_up[sourceNum-1].first > periodT) continue;

        for(int j=0; j<sourceNum; ++j) {
            vertexIds[j] = tmp_up[j].second;
            expected_up_time[vertexIds[j]] = tmp_up[j].first; // upd
            indegree[j] = 0;
        }

        // choose dest
        indegree[sourceNum] = 1; // dest
        tmp_dw.clear();
        for(int j=0; j<slaveCnt; ++j) {
            if(vis[j] || isNodeIdFail[j]) continue;
            tmp_dw.push_back({expected_dw_time[j] + 1.0 * blkSize / dw_bd[j], j});
        }
        sort(tmp_dw.begin(), tmp_dw.end());
        // -- check if time > periodT
        if(tmp_dw[0].first > periodT) withdraw_flag = 1;
        destId = tmp_dw[0].second;
        vertexIds[sourceNum] = destId; 
        expected_dw_time[destId] = tmp_dw[0].first; // upd

        // allocate in-degree
        int minDwId;
        for(int j=0; j<sourceNum-1 && !withdraw_flag; ++j) {
            minDwId = 0;
            tmp_dw.clear();
            for(int j=0; j<sourceNum+1; ++j) {
                pid = vertexIds[j];
                tmp_dw.push_back({expected_dw_time[pid] + 1.0 * blkSize / dw_bd[pid], pid});
            }

            for(int h=0; h<sourceNum+1; ++h) {
                if(tmp_dw[h].first < tmp_dw[minDwId].first) minDwId = h;
            }
            if(tmp_dw[minDwId].first > periodT) withdraw_flag = 1;
            expected_dw_time[vertexIds[minDwId]] = tmp_dw[minDwId].first; // upd
            ++indegree[minDwId];
        }

        if(withdraw_flag) { 
            cout << "withdraw stripe " << i << endl;
            for(int j=0; j<sourceNum+1; ++j) {
                pid = vertexIds[j];
                expected_dw_time[pid] = backup_dw[pid];
            }
            continue;
        }

        if(withdraw_flag) {
            cout << " withdraw but continue error" << endl;
            exit(-1);
        }

        cout << "degree of stripe " << i << " : in, out" << endl;
        for(int j=0; j<sourceNum+1; ++j) {
            cout << " " << indegree[j] << " " << outdegree[j];
            sumindeg[vertexIds[j]] += indegree[j];
            sumoutdeg[vertexIds[j]] += outdegree[j];
        } cout << endl;

        // given sources and dest and in/out-degree, construct the valid DAG
        vector<vector<int> > edges = constructDAG(sourceNum, indegree, outdegree);

        // form a stripe
        repairGraph G(sourceNum+1);
        for(int j=0; j<sourceNum+1; ++j) { 
            G.in[j] = (int*)malloc(sizeof(int) * (indegree[j] + 1));
            G.in[j][0] = 0;

            G.out[j] = (int*)malloc(sizeof(int) * (outdegree[j] + 1));
            G.out[j][0] = 0;
        }

        // edges
        int from, to;
        for(int j=0; j<edges.size(); ++j) { 
            for(int h=0; h<edges[j].size(); ++h) {
                from = j;
                to = edges[j][h];
                
                G.in[to][++G.in[to][0]] = from;
                G.out[from][++G.out[from][0]] = to;
            }
        }

        stripes.push_back(Stripe(G));

        for(int j=0; j<sourceNum+1; ++j) {
            //[del] vetex_to_peerNode
            stripes.back().vertex_to_peerNode[j] = vertexIds[j];
            stripes.back().vertex_to_peer[j] = vertexIds[j];
            stripes.back().peer_to_vertex[vertexIds[j]] = j;
        }

        batch_stripe_id.push_back(i);
        stripe_vis[i] = 1;
        --remain;
        if(_conf->_ecType == "LRC" && idxs[i] < eck+ec_LRC_L) outdegree[sourceNum] = 1;

    }

    cout << "remain cnt = " << remain << endl;
    for(int i=0; i<slaveCnt; ++i) {
        if(expected_up_time[i] > periodT && fabs(expected_up_time[i]-periodT) > eps) {
            cout << "expected_up_time > periodT " << endl;
            exit(-1);
        }
        if(expected_dw_time[i] > periodT && fabs(expected_dw_time[i]-periodT) > eps) {
            cout << expected_dw_time[i] << " " << periodT << " " << fabs(expected_dw_time[i]-periodT) << endl;
            cout << "expected_dw_time > periodT " << endl;
            exit(-1);
        }
        printf("node %d, up_time = %.3f, dw_time = %.3f \n", i, expected_up_time[i], expected_dw_time[i]);
    }

    int all_in = 0, all_out = 0;
    for(int i=0; i<slaveCnt; ++i) {
        printf("node %d, sum_dw = %d, sum_up = %d,  bd_dw = %d, bd_up = %d \n", i, sumindeg[i], sumoutdeg[i], dw_bd[i], up_bd[i]);
        all_in += sumindeg[i];
        all_out += sumoutdeg[i];
    }

    cout << "all: indeg = " << all_in << " , outdeg = " << all_out << endl;

    // check for correctness
    // unitTestForMapping(stripes, placement, failNodeId, slaveCnt, ecn, batch_stripe_id); (upd 22.5.3)
    unitTestForMapping(stripes, placement, isNodeIdFail, slaveCnt, ecn, batch_stripe_id);
    
    cout << "finish buildGraph!!!!!!!!!!" << endl;
    return stripes;
}
*/

vector<Stripe> LinesBuildGraph::buildFlexibleGraph(int& remain, int* stripe_vis, vector<int>& up_bd, vector<int>& dw_bd, int lostBlkCnt, int* placement, int* isSoureCandidate, int* idxs, map<int, int>& isNodeIdFail, int blkSize, vector<int>& batch_stripe_id) {
    // for stripes, choose sources and dest for each stripe
    // up_bd, dw_bd for each storage node
    // expected_up_time, expected_dw_time for each storage node
    vector<Stripe> stripes;

    assert(up_bd.size() == slaveCnt && dw_bd.size() == slaveCnt);

    vector<double> expected_up_time(slaveCnt, 0), expected_dw_time(slaveCnt, 0), backup_dw(slaveCnt);

    int pid, destId;
    vector<int> vertexIds(ecn+1), indegree(ecn+1), outdegree(ecn+1, 0);
    outdegree[eck] = 0;

    vector<int> sumindeg(slaveCnt, 0), sumoutdeg(slaveCnt, 0);

    vector<pair<double, int> > tmp_up(ecn-1), tmp_dw;
    int* vis = (int*)calloc(slaveCnt, sizeof(int));
    //add for cost flow
    int* select = (int*)calloc(slaveCnt, sizeof(int));
    for(int i=0; i<lostBlkCnt; ++i) {
        int withdraw_flag = 0;
        if(stripe_vis[i]) continue;
        for(int j=0; j<slaveCnt; ++j){
            vis[j] = 0;
            select[j] = -1;
        }
        // if LRC, judege whether local or global
        // choose sourceNum sources (eck for RS codes, eck or eck/ec_LRC_L for LRC codes)
        int sourceNum = eck;
        if(_conf->_ecType == "LRC" && idxs[i] < eck+ec_LRC_L) {
            // local
            sourceNum = ec_LRC_L ? eck/ec_LRC_L : 0;
            outdegree[sourceNum] = 0;
        }
        if(_conf->_ecType == "BUTTERFLY"){
            sourceNum = ecn-1;
            for(int j=0, ct=0; j<ecn; ++j) {
                pid = placement[i * ecn + j];
                vis[pid] = 1;
                // if(pid == failNodeId) continue; (upd 22.5.3) 
                if(isNodeIdFail[pid]) continue;
                if(!isSoureCandidate[i * ecn + j]) continue;
                tmp_up[ct++] = {expected_up_time[pid] + 1.0 * blkSize / up_bd[pid], pid};
                // prepare for withdraw
                backup_dw[pid] = expected_dw_time[pid];
            }
            sort(tmp_up.begin(), tmp_up.begin()+sourceNum);
            // -- check if time > periodT
            if(tmp_up[sourceNum-1].first > periodT) continue;
            // choose dest
            for(int i=0;i<sourceNum;i++)
                outdegree[i]=1;
            indegree[sourceNum] = ecn-1; // dest
            tmp_dw.clear();
            for(int j=0; j<slaveCnt; ++j) {
                if(vis[j] || isNodeIdFail[j]) continue;
                tmp_dw.push_back({expected_dw_time[j] + (ecn-1)*1.0 * blkSize / dw_bd[j], j});
            }
            sort(tmp_dw.begin(), tmp_dw.end());
            // -- check if time > periodT
            if(tmp_dw[0].first > periodT) continue;
            
            for(int j=0; j<sourceNum; ++j) {
                vertexIds[j] = tmp_up[j].second;
                expected_up_time[vertexIds[j]] = tmp_up[j].first; // upd
                indegree[j] = 0;
            }

            destId = tmp_dw[0].second;
            vertexIds[sourceNum] = destId; 
            expected_dw_time[destId] = tmp_dw[0].first; // upd
        }  
        //cout << "pid" << endl;
        else{
            for(int j=0, ct=0; j<ecn; ++j) {
                pid = placement[i * ecn + j];
                //cout << pid << " ";
                vis[pid] = 1;
                // if(pid == failNodeId) continue; (upd 22.5.3) 
                if(isNodeIdFail[pid]) continue;
                if(!isSoureCandidate[i * ecn + j]) continue;
                tmp_up[ct++] = {expected_up_time[pid] + 1.0 * blkSize / up_bd[pid], pid};
                // prepare for withdraw
                backup_dw[pid] = expected_dw_time[pid];
            }
            sort(tmp_up.begin(), tmp_up.begin()+sourceNum);
            // -- check if time > periodT
            if(tmp_up[sourceNum-1].first > periodT) continue;

            //add for cost flow
            int node_cnt=tmp_up.size();
            for(int j=sourceNum;j<tmp_up.size();j++)
                if(tmp_up[j].first > periodT){
                    node_cnt=j;
                    break;
                }
            
            indegree[sourceNum] = 1;
            
            destId=-1;
            double min_cost_flow=1e5+100;

            for(int j=0; j<slaveCnt; j++){
                if(vis[j] || isNodeIdFail[j]) continue;
                if((sumindeg[j]+1)*1.0/dw_bd[j]<min_cost_flow){
                    min_cost_flow=(sumindeg[j]+1)*1.0/dw_bd[j];
                    destId=j;
                }
            }
            if(min_cost_flow* blkSize > periodT) continue;
            vertexIds[sourceNum] = destId;
            double dw_up_alpha=0.2;
            vector<int> flow(node_cnt+1, 0);
            priority_queue<pair<double,int> > flow_cost;
            flow[node_cnt]=1;
            for(int j=0;j<node_cnt;j++){
                pid=tmp_up[j].second;
                flow_cost.push({dw_up_alpha*(sumoutdeg[pid]+1)*(-1.0)/up_bd[pid]
                                -(sumindeg[pid]+1)*1.0/dw_bd[pid],j});
            }
            flow_cost.push({(sumindeg[destId]+flow[node_cnt]+1)*(-1.0)/dw_bd[destId],node_cnt});

            for(int j=0;j<sourceNum-1;j++){
                pair<double,int> sl_flow=flow_cost.top();
                flow_cost.pop();
                pid = sl_flow.second;
                flow[sl_flow.second]++;
                flow_cost.push({(sumindeg[pid]+flow[pid]+1)*(-1.0)/dw_bd[pid],pid});
            }

            int sl_node_cnt=0;
            for(int j=0;j<node_cnt;j++)
                if(flow[j]!=0){
                    vertexIds[sl_node_cnt] = tmp_up[j].second;
                    select[j]=sl_node_cnt++;
                    //cout << j <<" " << select[j] << endl;
                }

            for(int j=0;j<node_cnt;j++){
                if(sl_node_cnt == sourceNum) break;
                if(select[j]==-1){
                    vertexIds[sl_node_cnt] = tmp_up[j].second;
                    select[j]=sl_node_cnt++;
                    //cout << j <<" " << select[j] << endl;
                }
            }

            int dw_flag=0;
            for(int j=0;j<node_cnt+1; j++){
                if(j!=node_cnt) pid=tmp_up[j].second;
                else pid=destId;     
                //cout << expected_dw_time[pid] + flow[j]*blkSize*1.0 / dw_bd[pid] << endl;
                if((expected_dw_time[pid] + flow[j]*blkSize*1.0 / dw_bd[pid]) > periodT){
                    dw_flag=1;
                    break;
                }
            }

            if(dw_flag) continue;

            for(int j=0; j<sourceNum+1; j++){
                indegree[j]=0;
                outdegree[j]=0;
            }

            for(int j=0;j<node_cnt;j++){
                if(select[j]!=-1){
                    outdegree[select[j]] = 1;
                    indegree[select[j]] += flow[j];
                    expected_up_time[tmp_up[j].second] = tmp_up[j].first;
                    expected_dw_time[tmp_up[j].second] += flow[j]*blkSize*1.0 / dw_bd[tmp_up[j].second];
                }
            }
            indegree[sourceNum]+=flow[node_cnt];
            expected_dw_time[destId] += flow[node_cnt]*blkSize*1.0 / dw_bd[destId];
        }

        cout << "degree of stripe " << i << " : in, out" << endl;
        for(int j=0; j<sourceNum+1; ++j) {
            
            cout << " " << indegree[j] << " " << outdegree[j] << endl;
            sumindeg[vertexIds[j]] += indegree[j];
            sumoutdeg[vertexIds[j]] += outdegree[j];
        } cout << endl;
        // given sources and dest and in/out-degree, construct the valid DAG
        vector<vector<int> > edges = constructDAG(sourceNum, indegree, outdegree);

        
        // form a stripe
        repairGraph G(sourceNum+1);
        for(int j=0; j<sourceNum+1; ++j) { 
            G.in[j] = (int*)malloc(sizeof(int) * (indegree[j] + 1));
            G.in[j][0] = 0;

            G.out[j] = (int*)malloc(sizeof(int) * (outdegree[j] + 1));
            G.out[j][0] = 0;
        }

        // edges
        int from, to;
        for(int j=0; j<edges.size(); ++j) { 
            for(int h=0; h<edges[j].size(); ++h) {
                from = j;
                to = edges[j][h];
                
                G.in[to][++G.in[to][0]] = from;
                G.out[from][++G.out[from][0]] = to;
            }
        }

        stripes.push_back(Stripe(G));

        for(int j=0; j<sourceNum+1; ++j) {
            //[del] vetex_to_peerNode
            stripes.back().vertex_to_peerNode[j] = vertexIds[j];
            stripes.back().vertex_to_peer[j] = vertexIds[j];
            stripes.back().peer_to_vertex[vertexIds[j]] = j;
        }

        batch_stripe_id.push_back(i);
        stripe_vis[i] = 1;
        --remain;
        if(_conf->_ecType == "LRC" && idxs[i] < eck+ec_LRC_L) outdegree[sourceNum] = 1;

        
    }

    cout << "remain cnt = " << remain << endl;
    for(int i=0; i<slaveCnt; ++i) {
        if(expected_up_time[i] > periodT && fabs(expected_up_time[i]-periodT) > eps) {
            cout << "expected_up_time > periodT " << endl;
            exit(-1);
        }
        if(expected_dw_time[i] > periodT && fabs(expected_dw_time[i]-periodT) > eps) {
            cout << expected_dw_time[i] << " " << periodT << " " << fabs(expected_dw_time[i]-periodT) << endl;
            cout << "expected_dw_time > periodT " << endl;
            exit(-1);
        }
        printf("node %d, up_time = %.3f, dw_time = %.3f \n", i, expected_up_time[i], expected_dw_time[i]);
    }

    int all_in = 0, all_out = 0;
    for(int i=0; i<slaveCnt; ++i) {
        printf("node %d, sum_dw = %d, sum_up = %d,  bd_dw = %d, bd_up = %d \n", i, sumindeg[i], sumoutdeg[i], dw_bd[i], up_bd[i]);
        all_in += sumindeg[i];
        all_out += sumoutdeg[i];
    }

    cout << "all: indeg = " << all_in << " , outdeg = " << all_out << endl;

    // check for correctness
    // unitTestForMapping(stripes, placement, failNodeId, slaveCnt, ecn, batch_stripe_id); (upd 22.5.3)
    unitTestForMapping(stripes, placement, isNodeIdFail, slaveCnt, ecn, batch_stripe_id);
    
    cout << "finish buildGraph!!!!!!!!!!" << endl;
    return stripes;
}