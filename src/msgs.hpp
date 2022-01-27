#ifndef TSTAMPED_MSGS_INCLUDED_
#define TSTAMPED_MSGS_INCLUDED_ 1

#include "msgs.hpp"

#include <functional>
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
  bool operator==(const vsrepl::MsgClientOp& o) const noexcept {
    return o.clientid == clientid && o.opstr == opstr && o.cliopid == cliopid;
  }
  std::size_t hash() const noexcept {
    auto h1 = std::hash<int>{}(clientid);
    auto h2 =  std::hash<std::string>{}(opstr);
    auto h3 =  std::hash<uint64_t>{}(cliopid);
    h1 ^= (h2 << 1);
    return h1 ^ (h3 << 1);
  }
};

struct MsgPrepare {
  int view;
  int op;
  int commit;
  std::size_t loghash;
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
  int view;
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

struct MsgPersistedCliOp {
  int view;
  uint64_t cliopid;
};

struct MsgOpPersistedQuery {
  MsgPersistedCliOp perscliop;
};

struct MsgOpPersistedResponse {
  std::string err;
  MsgPersistedCliOp perscliop;
  bool exists;
};
}

#endif
