#ifndef TSTAMPED_MSGS_INCLUDED_
#define TSTAMPED_MSGS_INCLUDED_ 1

#include <string>
#include <vector>

namespace vsrepl {

struct MsgClientOp {
    int clientid;
    std::string opstr;
};

struct MsgPrepare {
    int view;
    int op;
    int commit;
    std::string opstr;
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
    unsigned last_commit;
};

// Followers' response to the Leader candidate
struct MsgStartViewResponse {
    std::string err;
    int last_commit;
    int last_op;
    std::vector<std::pair<int, std::string>> missing_entries;
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
    std::string err;
    std::pair<int, std::string> op_log;
    std::vector<std::pair<int, std::string>> comitted_logs;
};

}

#endif
