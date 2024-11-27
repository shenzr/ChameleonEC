#ifndef _LINES_FNWORKER_HH_
#define _LINES_FNWORKER_HH_

// #define BLOCKPAUSE 1
#define ENABLE_PAUSE 1
#define DISABLE_PAUSE 0

#define ENABLE_CHANGE 1
#define DISABLE_CHANGE 0

#define CMDFLAG_REDIRECT -1
#define CMDFLAG_ADDPRE -2
#define CMDFLAG_FORWARD -3

#define NONE_PRE 1
#define HAVE_PRE 2

#define STATUS_UNDO 0
#define STATUS_DOING 1
#define STATUS_DONE 2
#define STATUS_RESENDTODEST 3
#define STATUS_READD 4

#include <set>
#include <string>
#include <cassert>
#include "DRWorker.hh"
#include "BUTTERFLY64Util.hh"

class LinesFullNodeWorker : public DRWorker {
    size_t _sendOnlyThreadNum;
    size_t _receiveAndSendThreadNum;
    thread _sendOnlyThrd;
    thread _receiveAndSendThrd;

    bool _sendWorkerFree;
    mutex _sendWorkerMtx;
    condition_variable _sendWorkerCv;

    bool _multiplyFree;
    mutex _multiplyMtx;
    condition_variable _multiplyCv;

    BUTTERFLYUtil* _butterflyUtil;
    int _decMatButterFly[6][20 * 8];
    int _lostBlkIdxBF;
    
    // for pausing task
    int _receivePauseFlag, _sendPauseFlag;
    map<string, int> isTaskCached;
    map<string, pair<char*, int> > blk2Cmd;
    vector<string> pausedBlks;
    map<string, int> retryCount;
    map<string, vector<string> > pausedTaskCheckCmd;
    redisContext* _coordinatorCtx;
    string localIPStr;
    
    int _pause_mode;
    int _change_mode;

    // for change structure
    map<string, int> blkPreFlag; // whether the blk has pre
    map<string, vector<unsigned int> > stragglerBlkPreIPs;
    map<string, int> stragglerBlkNextCnt;
    // map<string, unsigned int> stragglerBlkDestIp;
    // map<string, int> blkNextCnt;
    map<string, unsigned int> blkDestIp;
    map<string, int> blkStatus;
    map<string, int> blkRedirectFlag;
    map<string, set<unsigned int> > blkAddedPre;
    map<string, set<unsigned int> > blkReducedPre;
    mutex _blkStatusMtx;
    int retryThreshold = 5;

    void sendCmdToStuckPre(unsigned int, string);
    void sendCmdToDest(unsigned int, string, vector<unsigned int>);
    void sendCmdToForward(unsigned int, unsigned int, string);

    void readWorker(int&, variablesForTran& var, string, string, int, int, int, int);
    void pullWorker(int&, variablesForTran& var, redisContext*, unsigned int, int, int, string, int butterfly_id_in_stripe = 0, int butterfly_lost_blk_id_in_stripe = 0);
    void sendWorker(int&, variablesForTran& var, redisContext* rc, unsigned int senderIp, string lostBlkName, int, int);
    

    public: 
        LinesFullNodeWorker(Config* conf);
        void doProcess();
        void sendOnly(redisContext* selfCtx);
        void receiveAndSend(redisContext* selfCtx);
};

#endif  //_LINES_FNWORKER_HH_

