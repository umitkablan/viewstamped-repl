#ifndef TSTAMPED_MSGS_INCLUDED_
#define TSTAMPED_MSGS_INCLUDED_ 1

#include <string>
#include <vector>

namespace vsrepl {

struct MsgClientOp {
    int clientid;
    std::string opstr;
    uint64_t cliopid; // prevents opstr to re-execute together with clientid

    std::string toString() const {
        return std::to_string(clientid) + "/" + std::to_string(cliopid) + "/" + opstr;
    }
    bool operator==(const MsgClientOp& o) const {
        return o.clientid == clientid && o.opstr == opstr && o.cliopid == cliopid;
    }
};

struct MsgPrepare {
    int view;
    int op;
    int commit;
    MsgClientOp cliop;
};

struct MsgStartViewChange {
    int view;
};

struct MsgDoViewChange {
    int view;
};

// Leader's command to all possible Followers
struct MsgStartView {
    int view;
    int last_commit;
};

// Followers' response to the Leader candidate
struct MsgStartViewResponse {
    std::string err;
    int last_commit;
    std::vector<std::pair<int, MsgClientOp>> missing_entries;
};

struct MsgPrepareResponse {
    std::string err;
    int op;
};

struct MsgGetMissingLogs {
    int view;
    int my_last_commit;
};

struct MsgMissingLogsResponse {
    int view;
    std::string err;
    std::pair<int, MsgClientOp> op_log;
    std::vector<std::pair<int, MsgClientOp>> comitted_logs;
    unsigned hashoflast;
};

}

#endif
