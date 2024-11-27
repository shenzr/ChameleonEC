#include "LinesFullNodeWorker.hh"

string convertUNToStr(unsigned int ip) {
    string str = "";
    str += to_string(ip & 0xff);
    str += ".";
    str += to_string((ip >> 8) & 0xff);
    str += ".";
    str += to_string((ip >> 16) & 0xff);
    str += ".";
    str += to_string((ip >> 24) & 0xff);

    return str;
}

LinesFullNodeWorker::LinesFullNodeWorker(Config* conf) : DRWorker(conf) {
    _butterflyUtil = new BUTTERFLYUtil(conf);
    if(_conf->_ecType == "BUTTERFLY") {
        _butterflyUtil->generate_decoding_matrix(_decMatButterFly);
    }

    unsigned int coIP = _conf->_coordinatorIP;
    string rServer = convertUNToStr(coIP);

    cout << "coordinator server: " << rServer << endl;
    struct timeval timeout = {1, 500000};  // 1.5 seconds
    _coordinatorCtx = redisConnectWithTimeout(rServer.c_str(), 6379, timeout);
    if (_coordinatorCtx == NULL || _coordinatorCtx->err) {
        if (_coordinatorCtx) {
            cerr << "Connection error: " << _coordinatorCtx->errstr << endl;
            redisFree(_coordinatorCtx);
        } else {
            cerr << "Connection error: can't allocate redis context" << endl;
        }
        return;
    }

    _sendPauseFlag = 0;
    _receivePauseFlag = 0;
    blk2Cmd.clear();
    pausedBlks.clear();
    retryCount.clear();

    isTaskCached.clear();
    pausedTaskCheckCmd.clear();
    _pause_mode = (_conf->_scheduleMethod == SCHEDULE_METHOD_ONLINE) ? ENABLE_PAUSE : DISABLE_PAUSE;
    // _pause_mode = DISABLE_PAUSE;
    _change_mode = DISABLE_CHANGE;
    localIPStr = convertUNToStr(_conf->_localIP);

    if(_pause_mode == DISABLE_PAUSE)
        cout << "_pause_mode : DISABLE_PAUSE" << endl;
    else if(_pause_mode == ENABLE_PAUSE) 
        cout << "_pause_mode : ENABLE_PAUSE" << endl;


    // for change structure
    blkStatus.clear();
    blkRedirectFlag.clear();
    blkPreFlag.clear();
    stragglerBlkPreIPs.clear();
    blkDestIp.clear();
    // blkNextCnt.clear();
    stragglerBlkNextCnt.clear();
    blkAddedPre.clear();
    blkReducedPre.clear();

}

void LinesFullNodeWorker::sendOnly(redisContext* selfCtx) {
    redisReply* rReply;
    int repLen;
    char repStr[COMMAND_MAX_LENGTH];
    string lostBlkName, localBlkName;

    int ecK, coef;
    struct timeval t1, t2;
    while(true) {
        rReply = (redisReply*)redisCommand(selfCtx, "BLPOP cmd_send_only 10");
        if(rReply->type == REDIS_REPLY_NIL) {
            if (DR_WORKER_DEBUG)
                cout << typeid(this).name() << "::" << __func__ << "(): empty list" << endl;
        } else if(rReply->type == REDIS_REPLY_ERROR) {
            if (DR_WORKER_DEBUG)
                cout << typeid(this).name() << "::" << __func__ << "(): error happens" << endl;
        } else {
            repLen = rReply->element[1]->len;
            memcpy(repStr, rReply->element[1]->str, repLen);
            /** parse the cmd
             * | ecK(4Byte) | id(4Byte) | coefficient(4Byte)|
             * | pre_cnt(4Byte) = 0| pre 0 IP(4Byte) |...| pre pre_cnt-1 IP (4Byte) |    
             * | next_cnt(4Byte) | next 0 IP(4Byte) |...| next next_cnt-1 IP(4Byte) | 
             * | dest IP(4Byte) |(upd by LinesYao, 2022.03.04)
             * | lostFileNameLen(4Byte) | lostFIleName(l) | 
             * | localFileNameLen(4Btye) | localFileName(l') | 
             */
            int cmdoffset;
            int pre_cnt, next_cnt;
            int id_send;
            unsigned int tip, destIP;
            vector<unsigned int> nextSlaves;
            memcpy((char*)&ecK, repStr, 4); 
            memcpy((char*)&id_send, repStr + 4, 4);
            memcpy((char*)&coef, repStr + 8, 4);
            memcpy((char*)&pre_cnt, repStr + 12, 4);
            assert(pre_cnt == 0);
            cmdoffset = 16;
            
            memcpy((char*)&next_cnt, repStr + cmdoffset, 4); 
            cmdoffset += 4;
            for(int i=0; i<next_cnt; ++i) {
                memcpy((char*)&tip, repStr + cmdoffset, 4);
                nextSlaves.push_back(tip);
                cmdoffset += 4;
            }

            // parse dest IP (upd by LinesYao, 2022.03.04)
            memcpy((char*)&destIP, repStr + cmdoffset, 4);
            cmdoffset += 4;

            int lostBlkNameLen, localBlkNameLen;
            memcpy((char*)&lostBlkNameLen, repStr + cmdoffset, 4);
            lostBlkName = string(repStr + cmdoffset + 4, lostBlkNameLen);
            cmdoffset += 4 +lostBlkNameLen;
            memcpy((char*)&localBlkNameLen, repStr + cmdoffset, 4);
            localBlkName = string(repStr + cmdoffset + 4, localBlkNameLen);
            cmdoffset += 4 + localBlkNameLen;

            blkPreFlag[lostBlkName] = NONE_PRE;

            if(DR_WORKER_DEBUG) {
                cout << "parse cmd:" << endl;
                cout << "   ecK = " << ecK << endl;
                cout << "   id_in_stripe = " << id_send << endl;
                cout << "   coefficient = " << coef << endl;
                cout << "   no preSlave ";
                cout << endl << "   nextSlaveIPs: ";
                for(auto it : nextSlaves) 
                    cout << ip2Str(it) << "  ";
                cout << endl << ip2Str(destIP) << "  ";
                cout << endl << "   lostBlk = " << lostBlkName << ", localBlk = " << localBlkName << endl;
            }

            _sendPauseFlag = 0;
            
            if(blkRedirectFlag[lostBlkName]) {
                nextSlaves[0] = destIP;
            }

            _blkStatusMtx.lock();
            blkStatus[lostBlkName] = STATUS_DOING;
            _blkStatusMtx.unlock();

            /* readWorker */
            thread readThread([=] {readWorker(_sendPauseFlag, _send, localBlkName, lostBlkName, pre_cnt, id_send, ecK, coef);} );

            /* sendWorker */
            thread sendThread;
            if(!next_cnt) {
                assert(id_send == ecK);
                sendThread = thread([=] {sendWorker(_sendPauseFlag, _send, selfCtx, _localIP, lostBlkName, id_send, ecK);} ); 
            } else {
                sendThread = thread([=] {sendWorker(_sendPauseFlag, _send, findCtx(_send._slavesCtx, nextSlaves[0]), _localIP, lostBlkName, id_send, ecK);} );
            }

            readThread.join();
            sendThread.join();

            if(_change_mode == ENABLE_CHANGE) {
                _blkStatusMtx.lock();
                if(blkStatus[lostBlkName] == STATUS_RESENDTODEST) {
                    sendThread = thread([=] {sendWorker(_sendPauseFlag, _send, findCtx(_send._slavesCtx, destIP), _localIP, lostBlkName, id_send, ecK);} );
                    sendThread.join();
                }
                blkStatus[lostBlkName] = STATUS_DONE;
                _blkStatusMtx.unlock();
            }
            

            // send finish time
            gettimeofday(&t2, NULL);
            redisCommand(selfCtx, "RPUSH time-sec %lld", (long long)t2.tv_sec);
            redisCommand(selfCtx, "RPUSH time-usec %lld", (long long)t2.tv_usec);

            cleanup(_send);
        }
        freeReplyObject(rReply);
    }
}




void LinesFullNodeWorker::receiveAndSend(redisContext* selfCtx) {
    redisReply* rReply;
    int repLen;
    char repStr[COMMAND_MAX_LENGTH];
    string lostBlkName, localBlkName;
    char* cacheCmd;

    int ecK, coef;
    struct timeval t1, t2;
    RSUtil* rsu = new RSUtil();
    while(true) {
        int redoFlag = 0;
        if(pausedBlks.size()) {
            cout << "in paused vector" << endl;
            cout << "size " << pausedBlks.size() << endl;
            for(vector<string>::iterator it = pausedBlks.begin(); it != pausedBlks.end(); ++it) {
                cout << "name: " << *it << endl;
            }
            for(vector<string>::iterator it = pausedBlks.begin(); it != pausedBlks.end(); ++it) {
                int ok = 1;
                int i;
                for(i=0; i<pausedTaskCheckCmd[*it].size() && ok; ++i) {
                    // cout << "cmd "<< pausedTaskCheckCmd[*it][i] << "strlen of llencmd = " << strlen(pausedTaskCheckCmd[*it][i].c_str()) << endl;
                    rReply = (redisReply*)redisCommand(selfCtx, pausedTaskCheckCmd[*it][i].c_str());
                    
                    if(rReply->type == REDIS_REPLY_NIL || rReply->type == REDIS_REPLY_ERROR || rReply->integer < 1) ok = 0;
                    cout << "llen for pre_" << i << " " << *it << " " << rReply->integer << endl; 
                    freeReplyObject(rReply);
                }
                
                if(ok) {
                    /*
                    sz = blkAddedPre[*it].size();
                    for(auto stu : blkAddedPre[*it]) {
                        int ipd = (int)((stu >> 24) & 0xff);
                        string cmd = "LLEN tmp:" + *it + ":" + to_string(ipd);
                        rReply = (redisReply*)redisCommand(selfCtx, cmd.c_str());
                    
                        if(rReply->type == REDIS_REPLY_NIL || rReply->type == REDIS_REPLY_ERROR || rReply->integer < 1) ok = 0;
                        cout << "llen for pre_" << i << " " << *it << " " << rReply->integer << endl; 
                        
                        freeReplyObject(rReply);
                        if(!ok) break;
                    }
                    */
                 
                    redoFlag = 1;
                    cout << "redo " << *it << endl;
                    string info = localIPStr + "-" + *it;
                    rReply = (redisReply*)redisCommand(_coordinatorCtx, "RPUSH redos %b", info.c_str(), strlen(info.c_str()));
                    freeReplyObject(rReply);
                    // string s = string(blk2Cmds[*it].first, blk2Cmds[*it].second);

                    rReply = (redisReply*)redisCommand(selfCtx, "LPUSH cmd_receive %b", blk2Cmd[*it].first, blk2Cmd[*it].second);
                    pausedBlks.erase(it);
                    freeReplyObject(rReply);
                    break;
                } else {
                    ++retryCount[*it];
                    
                    cout << "retry " << *it << " for " << retryCount[*it] << " times" << endl;

                    if(retryCount[*it] > retryThreshold && _change_mode == ENABLE_CHANGE && stragglerBlkNextCnt[*it] != 0) { // change the RDAG of repair (upd by LinesYao, 2022.03.04) (upd by LinesYao, 2022.04.19)
                        // -- for stuck preSlave
                        cout << "change structure start" << endl;
                       
                        vector<unsigned int> blkStuckPre;
                        blkStuckPre.push_back(stragglerBlkPreIPs[*it][i-1]);
                        cout << "i=" << i << " sz = " << pausedTaskCheckCmd[*it].size() << " " << convertUNToStr(blkStuckPre[0]) << " " << convertUNToStr(stragglerBlkPreIPs[*it][i-1]) << endl;
                        for(; i<pausedTaskCheckCmd[*it].size(); ++i) {
                            rReply = (redisReply*)redisCommand(selfCtx, pausedTaskCheckCmd[*it][i].c_str());
                            
                            if(rReply->type == REDIS_REPLY_NIL || rReply->type == REDIS_REPLY_ERROR || rReply->integer < 1) {
                                blkStuckPre.push_back(stragglerBlkPreIPs[*it][i]);
                                
                            }
                            cout << "before change structure, check llen for pre_" << i << " " << *it << " " << rReply->integer << endl;  
                            freeReplyObject(rReply);
                        }
                        
                        for(auto preip : blkStuckPre) sendCmdToStuckPre(preip, *it);

                        sendCmdToDest(blkDestIp[*it], *it, blkStuckPre);
                        cout << localIPStr << " sendCmdToDest " << "  for blk " << *it << " to destip " << convertUNToStr(blkDestIp[*it]) << endl;

                        // change cmd and redo
                        for(auto stu : blkStuckPre) {
                            blkReducedPre[*it].insert(stu);
                        }
                        cout << "redo " << *it << endl;

                        rReply = (redisReply*)redisCommand(selfCtx, "LPUSH cmd_receive %b", blk2Cmd[*it].first, blk2Cmd[*it].second);
                        freeReplyObject(rReply);

                        string info = localIPStr + "-" + *it;
                        rReply = (redisReply*)redisCommand(_coordinatorCtx, "RPUSH redos %b", info.c_str(), strlen(info.c_str()));
                        freeReplyObject(rReply);

                        
                        redoFlag = 1;
                        pausedBlks.erase(it);
                        break;
                    }
                }
            }
        }


        rReply = (redisReply*)redisCommand(selfCtx, "BLPOP cmd_receive 1");
        if(rReply->type == REDIS_REPLY_NIL || rReply->type == REDIS_REPLY_ERROR) {
            cout << typeid(this).name() << "::" << __func__ << "(): empty list or error happens" << endl;
            freeReplyObject(rReply);
            continue;
        } 

        repLen = rReply->element[1]->len;
        memcpy(repStr, rReply->element[1]->str, repLen);
        freeReplyObject(rReply);

        int cmdFlag = 0;
        memcpy((char*)&cmdFlag, repStr, 4);
        
        if(cmdFlag == CMDFLAG_FORWARD) {
            exit(-1);
            // special command

            /* parse cmd
            * |cmdFlag (4 byte)| blkNameLen (4 byte)| blkName | IP who send cmd (4 byte)| 
            */
            
            int llen = 0;
            unsigned int preip;
            memcpy((char*)&llen, repStr + 4, 4);
            string blkName = string(repStr + 8, llen);
            memcpy((char*)&preip, repStr + 8 + llen, 4);

            
            redisContext* selfCtx = initCtx(0);

            for(int i=0; i<_packetCnt; ++i) _receive._diskFlag[i] = true;
            thread pullThread = thread([=] {
                pullWorker(_receivePauseFlag, _receive, selfCtx, preip, 0, 1, blkName);
            } );
            thread sendThread = thread([=] {sendWorker(_receivePauseFlag, _receive, findCtx(_receive._slavesCtx, blkDestIp[blkName]), preip, blkName, -1, ecK);} );

            pullThread.join();
            sendThread.join();
            redisFree(selfCtx);
            cleanup(_receive);
            continue;
        }

        /** parse the cmd
         * | ecK(4Byte) | id(4Byte) | coefficient(4Byte)|
         * | pre_cnt(4Byte) | pre 0 IP(4Byte) |...| pre pre_cnt-1 IP (4Byte) |    
         * | next_cnt(4Byte) | next 0 IP(4Byte) |...| next next_cnt-1 IP(4Byte) | 
         * | dest IP(4Byte) | (upd by LinesYao, 2022.03.04)
         * | lostFileNameLen(4Byte) | lostFIleName(l) | 
         * | localFileNameLen(4Btye) | localFileName(l') | 
         */
        int cmdoffset;
        int pre_cnt, next_cnt;
        int id_receive;
        unsigned int tip, destIP;
        vector<unsigned int> preSlaves, nextSlaves;
        memcpy((char*)&ecK, repStr, 4); 
        memcpy((char*)&id_receive, repStr + 4, 4);
        memcpy((char*)&coef, repStr + 8, 4);

        memcpy((char*)&pre_cnt, repStr + 12, 4);
        cmdoffset = 16 + 4 * pre_cnt;
        // for(int i=0; i<pre_cnt; ++i) {
        //     memcpy((char*)&tip, repStr + cmdoffset, 4);
        //     preSlaves.push_back(tip);
        //     cmdoffset += 4;
        // }

        memcpy((char*)&next_cnt, repStr + cmdoffset, 4); 
        cmdoffset += 4;
        for(int i=0; i<next_cnt; ++i) {
            memcpy((char*)&tip, repStr + cmdoffset, 4);
            nextSlaves.push_back(tip);
            cmdoffset += 4;
        }

        // parse destIP (upd by LinesYao, 2022.03.04)
        memcpy((char*)&destIP, repStr + cmdoffset, 4);
        cmdoffset += 4;

        int lostBlkNameLen, localBlkNameLen;
        memcpy((char*)&lostBlkNameLen, repStr + cmdoffset, 4);
        lostBlkName = string(repStr + cmdoffset + 4, lostBlkNameLen);

        cmdoffset += 4 + lostBlkNameLen;
        memcpy((char*)&localBlkNameLen, repStr + cmdoffset, 4);
        localBlkName = string(repStr + cmdoffset + 4, localBlkNameLen);
        cmdoffset += 4 + localBlkNameLen;

        blkPreFlag[lostBlkName] = HAVE_PRE;


        cmdoffset = 16;
        for(int i=0; i<pre_cnt; ++i) {
            memcpy((char*)&tip, repStr + cmdoffset, 4);
            if(!blkReducedPre[lostBlkName].count(tip)) {
                preSlaves.push_back(tip);
            }
            cmdoffset += 4;
        }
        pre_cnt = preSlaves.size();

        int added_done = blkAddedPre[lostBlkName].size();
        for(auto a : blkAddedPre[lostBlkName]) {
            preSlaves.push_back(a);
        }
        pre_cnt = preSlaves.size();
        
        if(DR_WORKER_DEBUG) {
            cout << "parse cmd:" << endl;
            cout << "   ecK = " << ecK << endl;
            cout << "   id_in_stripe = " << id_receive << endl;
            cout << "   coefficient = " << coef << endl;
            cout << "   preSlaveIPs: ";
            for(auto it : preSlaves) 
                cout << ip2Str(it) << "  ";
            cout << endl << "   nextSlaveIPs: ";
            for(auto it : nextSlaves) 
                cout << ip2Str(it) << "  ";
            cout << endl << ip2Str(destIP) << "  ";
            cout << endl << "   lostBlk = " << lostBlkName << ", localBlk = " << localBlkName << endl;
        }

        cacheCmd = (char*)calloc(sizeof(char), repLen); 
        memcpy(cacheCmd, repStr, repLen);
        blk2Cmd[lostBlkName] = {cacheCmd, repLen};

        int ok = 1;
        if(_pause_mode == ENABLE_PAUSE && !redoFlag) {
            
            for(int i=0; i<pre_cnt && ok; ++i) {
                int ipd = (int)((preSlaves[i] >> 24) & 0xff);
                string cmd = "LLEN tmp:" + lostBlkName + ":" + to_string(ipd);
                rReply = (redisReply*)redisCommand(selfCtx, cmd.c_str());
                        
                if(rReply->type == REDIS_REPLY_NIL || rReply->type == REDIS_REPLY_ERROR || rReply->integer < 1) ok = 0;
                cout << "llen for " <<lostBlkName << " " << rReply->integer << endl; 
                freeReplyObject(rReply);
            }

            if(!ok) {
                if(!isTaskCached[lostBlkName]) {
                    // cacheCmd = (char*)calloc(sizeof(char), repLen); 
                    // memcpy(cacheCmd, repStr, repLen);
                    // blk2Cmds[lostBlkName] = {cacheCmd, repLen};
                    pausedBlks.push_back(lostBlkName);
                    ++retryCount[lostBlkName];
                    if(pausedTaskCheckCmd[lostBlkName].size()) pausedTaskCheckCmd[lostBlkName].clear();
                    if(stragglerBlkPreIPs[lostBlkName].size()) stragglerBlkPreIPs[lostBlkName].clear();
                    for(int i=0; i<pre_cnt; ++i) {
                        stragglerBlkPreIPs[lostBlkName].push_back(preSlaves[i]);
                        int ipd = (int)((preSlaves[i] >> 24) & 0xff);
                        string cmd = "LLEN tmp:" + lostBlkName + ":" + to_string(ipd);
                        pausedTaskCheckCmd[lostBlkName].push_back(cmd);
                    }
                    blkDestIp[lostBlkName] = destIP;
                    stragglerBlkNextCnt[lostBlkName] = next_cnt;
                    isTaskCached[lostBlkName] = 1;
                }
            }
        }

        blkDestIp[lostBlkName] = destIP;

        if(!ok) continue;
        _receivePauseFlag = 0;
        
        cout << "start " << lostBlkName << endl;

        

        if(blkRedirectFlag[lostBlkName]) {
            nextSlaves[0] = destIP;
        }

        // for(auto a : blkAddedPre[lostBlkName]) {
        //     preSlaves.push_back(a);
        // }
        // pre_cnt = preSlaves.size();

        _blkStatusMtx.lock();
        blkStatus[lostBlkName] = STATUS_DOING;
        _blkStatusMtx.unlock();

        

        /* readWorker */
        thread readThread([=] {readWorker(_receivePauseFlag, _receive, localBlkName, lostBlkName, pre_cnt, id_receive, ecK, coef);} );

        /* pullWorker */

        _receive._pktsWaiting = (int*)calloc(sizeof(int), pre_cnt);
        thread pullThread[pre_cnt];
        vector<redisContext*> selfCtxs;
        if(_conf->_ecType == "BUTTERFLY") {
            int ids = coef;
            int lostBlkIdx = ids & 7;
            for(int i=0, off=15; i<pre_cnt; ++i, off-=3) {
                selfCtxs.push_back(initCtx(0));
                pullThread[i] = thread([=] {
                    pullWorker(_receivePauseFlag, _receive, selfCtxs[i], preSlaves[i], i, pre_cnt, lostBlkName, (ids & (7<<off)) >> off, lostBlkIdx);
                } );
            }
        } else {
            for(int i=0; i<pre_cnt; ++i) {
                selfCtxs.push_back(initCtx(0));
                pullThread[i] = thread([=] {
                    pullWorker(_receivePauseFlag, _receive, selfCtxs[i], preSlaves[i], i, pre_cnt, lostBlkName);
                } );
            }
        }
        
        /* sendWorker */
        thread sendThread;
        if(!next_cnt) {
            assert(id_receive == ecK);
            if(DR_WORKER_DEBUG) cout << "_id = " << id_receive << endl;
            sendThread = thread([=] {sendWorker(_receivePauseFlag, _receive, selfCtx, _localIP, lostBlkName, id_receive, ecK);} );

        } else {
            sendThread = thread([=] {sendWorker(_receivePauseFlag, _receive, findCtx(_receive._slavesCtx, nextSlaves[0]), _localIP, lostBlkName, id_receive, ecK);} );
        }

        readThread.join();
        cout << lostBlkName << " readThreads end" << endl;
        for(int i=0; i<pre_cnt; ++i) pullThread[i].join();     
        cout << lostBlkName << " pullThread end" << endl;   
        sendThread.join();
        cout << lostBlkName << " sendThread end" << endl;


        if(_change_mode == ENABLE_CHANGE) {
            _blkStatusMtx.lock();

            if(blkStatus[lostBlkName] == STATUS_RESENDTODEST) {
                sendThread = thread([=] {sendWorker(_receivePauseFlag, _receive, findCtx(_receive._slavesCtx, destIP), _localIP, lostBlkName, id_receive, ecK);} );
                sendThread.join();
            } else if(blkStatus[lostBlkName] == STATUS_READD) {
                
                _receive._pktsWaiting = (int*)calloc(sizeof(int), pre_cnt);
                _receive._waitingToSend = 0;
                int added = blkAddedPre[lostBlkName].size(), i=0;
                thread addedPullThread[added];
                for(auto apre : blkAddedPre[lostBlkName]) {
                    selfCtxs.push_back(initCtx(0));
                    addedPullThread[i] = thread([=] {
                        pullWorker(_receivePauseFlag, _receive, selfCtxs[i], apre, i, added, lostBlkName);
                    } );
                    ++i;
                }
                sendThread = thread([=] {sendWorker(_receivePauseFlag, _receive, selfCtx, _localIP, lostBlkName, id_receive, ecK);} );

                for(int i=0; i<added; ++i) addedPullThread[i].join();
                sendThread.join();

                for(int i=pre_cnt; i<pre_cnt+added; ++i) redisFree(selfCtxs[i]);
            }
            blkStatus[lostBlkName] = STATUS_DONE;
            _blkStatusMtx.unlock();
        }
        

        for(int i=0; i<pre_cnt; ++i) redisFree(selfCtxs[i]);

        // send finish time
        gettimeofday(&t2, NULL);
        redisCommand(selfCtx, "RPUSH time-sec %lld", (long long)t2.tv_sec);
        redisCommand(selfCtx, "RPUSH time-usec %lld", (long long)t2.tv_usec);

        cleanup(_receive);
    }
}

void LinesFullNodeWorker::sendCmdToStuckPre(unsigned int preIP, string blkName) {
    cout << localIPStr << " " << __func__ << "  for blk " << blkName << " to preip " << convertUNToStr(preIP) << endl;
    char* cmd = (char*)calloc(sizeof(char), COMMAND_MAX_LENGTH); 
    /* cmd
     * |cmdFlag (4 byte)| blkNameLen (4 byte)| blkName | IP who send cmd (4 byte)|
     */
    int cmdFlag = CMDFLAG_REDIRECT, llen;
    memcpy(cmd, (char*)&cmdFlag, 4);
    llen = strlen(blkName.c_str());
    memcpy(cmd + 4, (char*)&llen, 4); 
    memcpy(cmd + 8, blkName.c_str(), llen);
    memcpy(cmd + 8 + llen, (char*)&_localIP, 4);

    redisReply* rReply = (redisReply*)redisCommand(findCtx(_receive._slavesCtx, preIP), "RPUSH dr_cmds %b", cmd, 12+llen);
    freeReplyObject(rReply);


    string info = localIPStr + "-" + blkName;
    rReply = (redisReply*)redisCommand(_coordinatorCtx, "RPUSH sendCmdToPre %b", info.c_str(), strlen(info.c_str()));
    freeReplyObject(rReply);
}

void LinesFullNodeWorker::sendCmdToDest(unsigned int destIP, string blkName, vector<unsigned int> blkStuckPre) {
    char* cmd = (char*)calloc(sizeof(char), COMMAND_MAX_LENGTH); 
    /* cmd
     * |cmdFlag (4 byte)| blkNameLen (4 byte)| blkName | 
     * | addedPreCnt (4 byte)| pre 0 IP |... 
     */
    int cmdFlag = CMDFLAG_ADDPRE, sz = blkStuckPre.size(), llen;
    memcpy(cmd, (char*)&cmdFlag, 4);
    llen = strlen(blkName.c_str());
    memcpy(cmd + 4, (char*)&llen, 4); 
    memcpy(cmd + 8, blkName.c_str(), llen);
    int offset = 8 + llen;
    memcpy(cmd + offset, (char*)&sz, 4);
    offset += 4;
    for(auto pre : blkStuckPre) {
        memcpy(cmd + offset, (char*)&pre, 4);
        offset += 4;
    }

    redisReply* rReply = (redisReply*)redisCommand(findCtx(_receive._slavesCtx, destIP), "RPUSH dr_cmds %b", cmd, offset);
    freeReplyObject(rReply);

    string info = localIPStr + "-" + blkName;
    rReply = (redisReply*)redisCommand(_coordinatorCtx, "RPUSH sendCmdToDest %b", info.c_str(), strlen(info.c_str()));
    freeReplyObject(rReply);
}

void LinesFullNodeWorker::sendCmdToForward(unsigned int nextIP, unsigned int destIP, string blkName) {
    char* cmd = (char*)calloc(sizeof(char), COMMAND_MAX_LENGTH); 
    /* cmd
     * |cmdFlag (4 byte)| blkNameLen (4 byte)| blkName |  IP who send cmd (4 byte)| 
     */
    int cmdFlag = CMDFLAG_FORWARD, llen;
    memcpy(cmd, (char*)&cmdFlag, 4);
    llen = strlen(blkName.c_str());
    memcpy(cmd + 4, (char*)&llen, 4); 
    memcpy(cmd + 8, blkName.c_str(), llen);
    memcpy(cmd + 8 + llen, (char*)&_localIP, 4);

    redisReply* rReply = (redisReply*)redisCommand(findCtx(_receive._slavesCtx, nextIP), "RPUSH dr_cmds %b", cmd, 8+llen);
    freeReplyObject(rReply);
}


void LinesFullNodeWorker::doProcess() {
    cout << __func__ << " starts" << endl;
    

    _multiplyFree = true;
    _sendWorkerFree = true;
    _sendOnlyThreadNum = 1;
    _receiveAndSendThreadNum = 1;
    redisContext* _selfCtxForSend = initCtx(0);
    redisContext* _selfCtxForReceive = initCtx(0);

    _sendOnlyThrd = thread([=] { this->sendOnly(_selfCtxForSend); });
    _receiveAndSendThrd = thread([=] { this->receiveAndSend(_selfCtxForReceive); });


    redisReply* rReply;
    int repLen;
    char repStr[COMMAND_MAX_LENGTH];
    int linesBeginFlag = 0;
    while(true) {
        rReply = (redisReply*)redisCommand(_selfCtx, "BLPOP dr_cmds 100");
        if(rReply->type == REDIS_REPLY_NIL) {
            if (DR_WORKER_DEBUG)
                cout << typeid(this).name() << "::" << __func__ << "(): empty list" << endl;
        } else if(rReply->type == REDIS_REPLY_ERROR) {
            if (DR_WORKER_DEBUG)
                cout << typeid(this).name() << "::" << __func__ << "(): error happens" << endl;
        } else {
            repLen = rReply->element[1]->len;
            memcpy(repStr, rReply->element[1]->str, repLen);

            // determine whether cmd belong to CMDFLAG_REDIRECT
            int cmdFlag = 0;
            memcpy((char*)&cmdFlag, repStr, 4);
            if(cmdFlag == CMDFLAG_REDIRECT) {
                cout << "cmdFlag == CMDFLAG_REDIRECT" << endl;
                /* cmd
                * |cmdFlag (4 byte)| blkNameLen (4 byte)| blkName | IP who send cmd (4 byte)|
                */
                int len = 0;
                memcpy((char*)&len, repStr + 4, 4);
                string blkName = string(repStr + 8, len);

                _blkStatusMtx.lock();
                cout << "blkStatus = " << blkStatus[blkName] << endl;

                if(blkStatus[blkName] == STATUS_DONE) exit(-1);

                if(!blkPreFlag.count(blkName) || blkStatus[blkName] == STATUS_UNDO) {
                    blkRedirectFlag[blkName] = 1;
                } else if(blkStatus[blkName] == STATUS_DOING) {
                    cout << "doing_redirect " << blkName << endl;
                    string info = localIPStr + "-" + blkName;
                    redisReply* rrReply = (redisReply*)redisCommand(_coordinatorCtx, "RPUSH doing_redirect %b", info.c_str(), strlen(info.c_str()));
                    freeReplyObject(rrReply);

                    blkStatus[blkName] = STATUS_RESENDTODEST;
                } else if(blkStatus[blkName] == STATUS_DONE) {
                    unsigned int ip;
                    memcpy((char*)&ip, repStr + 8 + len, 4);
                    sendCmdToForward(ip, blkDestIp[blkName], blkName);
                } 
                _blkStatusMtx.unlock();
                freeReplyObject(rReply);
                cout << "cmdFlag == CMDFLAG_REDIRECT ends" << endl;
                continue;
            } else if(cmdFlag == CMDFLAG_FORWARD) {
                cout << "cmdFlag == CMDFLAG_FORWARD" << endl;
                redisCommand(_selfCtx, "RPUSH cmd_receive %b", repStr, repLen);
                freeReplyObject(rReply);
                cout << "cmdFlag == CMDFLAG_FORWARD ends" << endl;
                continue;
            } else if(cmdFlag == CMDFLAG_ADDPRE) {
                cout << "cmdFlag == CMDFLAG_ADDPRE" << endl;
                /* cmd
                * |cmdFlag (4 byte)| blkNameLen (4 byte)| blkName | 
                * | addedPreCnt (4 byte)| pre 0 IP |... 
                */
                int len = 0, added = 0, offset;
                memcpy((char*)&len, repStr + 4, 4);
                string blkName = string(repStr + 8, len);
                memcpy((char*)&added, repStr + 8 + len, 4);
                offset = 12 + len;
                unsigned int ip;
                for(int i=0; i<added; ++i) {
                    memcpy((char*)&ip, repStr + offset, 4);
                    blkAddedPre[blkName].insert(ip);

                    int ipd = (int)((ip >> 24) & 0xff);
                    string cmd = "LLEN tmp:" + blkName + ":" + to_string(ipd);
                    pausedTaskCheckCmd[blkName].push_back(cmd);
                    stragglerBlkPreIPs[blkName].push_back(ip);
                    cout << convertUNToStr(ip) << " ";
                    offset += 4;
                }
                cout << endl;

                _blkStatusMtx.lock();
                cout << "blkStatus = " << blkStatus[blkName] << endl;

                if(!blkPreFlag.count(blkName) || blkStatus[blkName] == STATUS_UNDO) {
                    cout << "blkname = " << blkName << ",  blkAddedPre.size = " << blkAddedPre[blkName].size() << endl;
                } else if(blkStatus[blkName] == STATUS_DOING) {
                    cout << "doing_addpre " << blkName << endl;
                    string info = localIPStr + "-" + blkName;
                    redisReply* rrReply = (redisReply*)redisCommand(_coordinatorCtx, "RPUSH doing_addpre %b", info.c_str(), strlen(info.c_str()));
                    freeReplyObject(rrReply);

                    blkStatus[blkName] = STATUS_READD;
                } else if(blkStatus[blkName] == STATUS_DONE) {
                    // [TODO]
                    exit(-1);
                }
                _blkStatusMtx.unlock();
                freeReplyObject(rReply);
                cout << "cmdFlag == CMDFLAG_ADDPRE end" << endl;
                continue;
            }

            int pre_cnt;
            memcpy((char*)&pre_cnt, rReply->element[1]->str + 12, 4);
            if(pre_cnt) { // 2
                redisCommand(_selfCtx, "RPUSH cmd_receive %b", repStr, repLen);
            } else { // 1
                redisCommand(_selfCtx, "RPUSH cmd_send_only %b", repStr, repLen);
            }
        }
        freeReplyObject(rReply);
    }

    // should not reach here
    _sendOnlyThrd.join();
    _receiveAndSendThrd.join();
}

void LinesFullNodeWorker::pullWorker(int& pauseFlag, variablesForTran& var, redisContext* rc, unsigned int senderIp, int tid, int thread_num, string loskBLkName, int bfid, int bflid) {
    if(DR_WORKER_DEBUG) cout << __func__ << " starts " << endl;

    int finishedPkt;
    redisReply* rReply;
    if(_conf->_ecType == "BUTTERFLY") {
        int subBlockIndex, subPktCnt = _packetCnt / 8;
        vector<vector<int>> contributions(4, vector<int>());
        int* xorNeededCnt = (int*)calloc(8*subPktCnt, sizeof(int));
        int idBegin = (bflid < bfid) ? ((bfid-1)*4) : (bfid*4); 
        for(int i=idBegin; i<idBegin+4; ++i) {
            for(int j=0; j<8; ++j) {
                if(_decMatButterFly[bflid][j*20+i]) {
                    contributions[i-idBegin].push_back(j);
                    for(int z=j*subPktCnt; z<(j+1)*subPktCnt; ++z) ++xorNeededCnt[z];
                }
            }
        }
        
        for(int i=0; i<_packetCnt/2; ++i) {
            subBlockIndex = i / subPktCnt;
            while(!var._diskFlag[i]) {
                unique_lock<mutex> lck(var._diskMtx[i]);
                var._diskCv.wait(lck);
            }
            int ipd = (int)((senderIp >> 24) & 0xff);
            rReply = (redisReply*)redisCommand(rc, "BLPOP tmp:%s:%d 0", loskBLkName.c_str(), ipd);
            
            int sz = contributions[subBlockIndex].size(), xorOff;
            for(int j=0; j<sz; ++j) {
                xorOff = contributions[subBlockIndex][j]*subPktCnt + i%subPktCnt;
                var._diskCalMtx[xorOff].lock();
                Computation::XORBuffers(var._diskPkts[xorOff], rReply->element[1]->str, _packetSize);
                var._diskCalMtx[xorOff].unlock();
                --xorNeededCnt[xorOff];
            }
            freeReplyObject(rReply);

            finishedPkt = var._pktsWaiting[tid];
            while(finishedPkt<_packetCnt && (!xorNeededCnt[finishedPkt])) ++finishedPkt;
            var._pktsWaiting[tid] = finishedPkt;

            var._pullWorkerMtx.lock();
            for(int j=0; j<thread_num; ++j) finishedPkt = min(finishedPkt, var._pktsWaiting[j]);
            if(finishedPkt > var._waitingToSend) {
                var._waitingToSend = finishedPkt;
                unique_lock<mutex> lck(var._mainSenderMtx);
                var._mainSenderCondVar.notify_all();
            }
            var._pullWorkerMtx.unlock();
        }
    } else {
        for(int i=0; i<_packetCnt; ++i) {
            while(!var._diskFlag[i]) {
                unique_lock<mutex> lck(var._diskMtx[i]);
                var._diskCv.wait(lck);
            }

            int ipd = (int)((senderIp >> 24) & 0xff);

            rReply = (redisReply*)redisCommand(rc, "BLPOP tmp:%s:%d 0", loskBLkName.c_str(), ipd);
               
            var._diskCalMtx[i].lock();
            Computation::XORBuffers(var._diskPkts[i], rReply->element[1]->str, _packetSize);
            var._diskCalMtx[i].unlock();

            freeReplyObject(rReply);
            ++var._pktsWaiting[tid];
            finishedPkt = var._pktsWaiting[tid];

            var._pullWorkerMtx.lock();
            for(int j=0; j<thread_num; ++j) finishedPkt = min(finishedPkt, var._pktsWaiting[j]);
            if(finishedPkt > var._waitingToSend) {
                var._waitingToSend = finishedPkt;
                unique_lock<mutex> lck(var._mainSenderMtx);
                var._mainSenderCondVar.notify_all();
            }
            var._pullWorkerMtx.unlock();
            if(i%16==0) cout << ipd << " " << typeid(this).name() << "::" << __func__ << "() processing pkt " << i << " for blk " << loskBLkName << endl; 
        }
    }
}


void LinesFullNodeWorker::readWorker(int& pauseFlag, variablesForTran& var, string blkName, string lostBlkName, int preSlaveCnt, int _id, int _ecK, int _coefficient) { // read from local storage

    string blkPath = _conf->_blkDir + '/' + blkName;
    int fd = open(blkPath.c_str(), O_RDONLY);
    if(fd == -1) {
        blkPath = "";
        bool isFind = false;
        readFile(_conf->_blkDir, blkName, blkPath, isFind);
        fd = open(blkPath.c_str(), O_RDONLY);
        if(fd == -1) {
            cout << "Test by LinesYao: fd: " << fd << endl;
            exit(1);
        }
    }
    
    if(DR_WORKER_DEBUG) cout << typeid(this).name() << "::" << __func__ << "() openLocal fd = " << fd << endl;

    int base = _conf->_packetSkipSize;

    if(_conf->_ecType == "BUTTERFLY") {
        int alpha = 1<<(_conf->_ecK -1); // alpha = 2^(k-1)
        int sub_pkt_cnt = _packetCnt / alpha;
        char* buffer = (char*)malloc(sizeof(char) * _packetSize);
        vector<int> subBlockIndex;
        for(int subId=0, off=(alpha/2-1)*alpha; subId<alpha/2; ++subId, off-=alpha) {
            subBlockIndex.clear();
            for(int j=0; j<alpha; ++j) {
                if(_coefficient & (1<<(off+alpha-1-j))) subBlockIndex.push_back(j);
            }

            int cnt = subBlockIndex.size(), pktid;
            for(int pkt=0; pkt<sub_pkt_cnt; ++pkt) { 
                pktid = subId*sub_pkt_cnt+pkt;
                if(_id != _ecK) {
                    for(int i=0; i<cnt; ++i) {
                        base = _conf->_packetSkipSize + subBlockIndex[i] * sub_pkt_cnt * _packetSize;
                        if(!i) {
                            readPacketFromDisk(fd, var._diskPkts[pktid], _packetSize, base + pkt * _packetSize);
                        } else {
                            readPacketFromDisk(fd, buffer, _packetSize, base + pkt * _packetSize);
                            Computation::XORBuffers(var._diskPkts[pktid], buffer, _packetSize);
                        }
                    }
                }
                
                {
                    unique_lock<mutex> lck(var._diskMtx[pktid]);
                    var._diskFlag[pktid] = true;
                    var._diskCv.notify_all(); // linesyao one()->all()
                    if(!preSlaveCnt) {
                        ++var._waitingToSend;
                        var._mainSenderCondVar.notify_one();
                    }
                }
            }
        }
        
        free(buffer);
    } else {
        for(int i=0; i<_packetCnt; ++i) {
            if(_id != _ecK) {
                readPacketFromDisk(fd, var._diskPkts[i], _packetSize, base);
                while(_multiplyFree == false) {
                    unique_lock<mutex> lck(_multiplyMtx);
                    _multiplyCv.wait(lck);
                }
                {
                    unique_lock<mutex> lck(_multiplyMtx);
                    _multiplyFree = false;
                }
                RSUtil::multiply(var._diskPkts[i], _coefficient, _packetSize);
                {
                    unique_lock<mutex> lck(_multiplyMtx);
                    _multiplyFree = true;
                    _multiplyCv.notify_all();

                }
            }
            base += _packetSize;
            {
                unique_lock<mutex> lck(var._diskMtx[i]);
                var._diskFlag[i] = true;
                var._diskCv.notify_all(); 
                if(!preSlaveCnt) {
                    ++var._waitingToSend;
                    var._mainSenderCondVar.notify_all();
                }
            }
 
            if(DR_WORKER_DEBUG && i%16==0) cout << typeid(this).name() << "::" << __func__ << "() processing pkt " << i << " for local blk " << blkName << endl; 
        }
    }

    close(fd);
}

void LinesFullNodeWorker::sendWorker(int& pauseFlag, variablesForTran& var, redisContext* rc, unsigned int ip, string lostBlkName, int _id, int _ecK) {
    if(DR_WORKER_DEBUG) cout << __func__ << " starts " << endl;
    redisReply* rReply;

    if(_id == _ecK) {
        ofstream ofs(_conf->_stripeStore+"/"+lostBlkName);
        for(int i=0; i<_packetCnt; ++i) {
            while(i >= var._waitingToSend) {
                unique_lock<mutex> lck(var._mainSenderMtx);
                var._mainSenderCondVar.wait(lck);
            }

            if(_conf->_fileSysType == "HDFS3") {
                rReply = (redisReply*)redisCommand(rc, "RPUSH %s %b", lostBlkName.c_str(), var._diskPkts[i], _packetSize);
                freeReplyObject(rReply);
            } else {
                ofs.write(var._diskPkts[i], _packetSize);
            }
            if(DR_WORKER_DEBUG && i%16==0) 
                cout << typeid(this).name() << "::" << __func__ << "() processing pkt " << i << " for blk " << lostBlkName << endl;
        }
        ofs.close();
        if(_conf->_fileSysType == "HDFS3") { 
            redisCommand(rc, "RPUSH ACK %s", lostBlkName.c_str());
        } else {
            redisCommand(rc, "RPUSH %s ack", lostBlkName.c_str()); // [linesyao] finished ack!
        }
        // send finish ack
        redisCommand(_coordinatorCtx, "RPUSH finishedBlks %b", lostBlkName.c_str(), strlen(lostBlkName.c_str()));
    } else {
        
        for(int i=0; i<_packetCnt; ++i) {
            if(_conf->_ecType == "BUTTERFLY" && i==_packetCnt/2) break;
            while(i >= var._waitingToSend) {
                unique_lock<mutex> lck(var._mainSenderMtx);
                var._mainSenderCondVar.wait(lck);
            }

            int ipd = (int)((ip >> 24) & 0xff);

            rReply = (redisReply*)redisCommand(rc, "RPUSH tmp:%s:%d %b", lostBlkName.c_str(), ipd, var._diskPkts[i], _packetSize);
            freeReplyObject(rReply);

            if(DR_WORKER_DEBUG && i==63) 
                cout << typeid(this).name() << "::" << __func__ << "() processing pkt " << i << " for blk " << lostBlkName << endl;
        }
    }
}


