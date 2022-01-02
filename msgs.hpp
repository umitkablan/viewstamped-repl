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

struct MsgStartView {
    int view;
    unsigned last_commit;
};

struct MsgStartViewResponse {
    std::string err;
    int last_commit;
    std::vector<std::string> missing_entries;
};

struct MsgPrepareResponse {
    std::string err;
    int op;
};
}

#endif
