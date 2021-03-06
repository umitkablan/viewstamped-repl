#ifndef TSTAMPED_MSGS_INCLUDED_
#define TSTAMPED_MSGS_INCLUDED_ 1

#include "msgs.hpp"

#include <functional>
#include <string>
#include <vector>

namespace vsrepl {

struct MsgClientOp {
  unsigned clientid;
  std::string opstr;
  uint64_t cliopid; // prevents opstr to re-execute together with clientid
  bool dont_notify;

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

struct MsgLeaderRedirect {
    unsigned view;
    unsigned leader;
};

struct MsgPrepare {
  unsigned view;
  int op;
  int commit;
  std::size_t loghash;
  MsgClientOp cliop;
};

struct MsgStartViewChange {
  unsigned view;
};

struct MsgDoViewChange {
  unsigned view;
};

// Leader's command to all possible Followers
struct MsgStartView {
  unsigned view;
  int last_commit;
};

// Followers' response to the Leader candidate
struct MsgStartViewResponse {
  unsigned view;
  std::string err;
  int last_commit;
  std::vector<std::pair<int, MsgClientOp>> missing_entries;
};

struct MsgPrepareResponse {
  std::string err;
  int op;
};

struct MsgGetMissingLogs {
  unsigned view;
  int my_last_commit;
};

struct MsgMissingLogsResponse {
  unsigned view;
  std::string err;
  std::pair<int, MsgClientOp> op_log;
  std::vector<std::pair<int, MsgClientOp>> comitted_logs;
  std::size_t tothash;
};

struct MsgPersistedCliOp {
  unsigned view;
  uint64_t cliopid;
};

struct MsgOpPersistedQuery {
  MsgPersistedCliOp perscliop;
};

}

#endif
