#ifndef VSREPL_CLI_INCLUDED_
#define VSREPL_CLI_INCLUDED_ 1

#include "msgs.hpp"

#include <unordered_map>

namespace vsrepl
{

template <typename TMsgDispatcher>
class VSReplCli
{
public:
  enum class OpState : char {
    DoesntExist = 1,
    JustStarted = 7,
    Ongoing,
    Consumed,
  };

  VSReplCli(unsigned client_id, TMsgDispatcher& dp, int totreplicas);

  // returns a unique opID associated with opstr
  unsigned InitOp(const std::string& opstr);
  // idempotent operation, could be used to get status
  OpState StartOp(unsigned opID);
  // clean op's state iff operation is consumed, in order to free memory
  int DeleteOpID(unsigned opID);

  void ConsumeCliMsg(int from, const MsgPersistedCliOp&);
  int ConsumeReply(int from, const MsgOpPersistedResponse&);
  void ConsumeReply(int from, const MsgLeaderRedirect&);

private:
  struct opStruct {
    std::string str;
    OpState st;
    unsigned tot_received;
  };

  void setView(int view);

private:
  const unsigned client_id_;
  TMsgDispatcher& dispatcher_;
  const int totreplicas_;
  const int consensus_min_;

  int last_view_;
  unsigned last_op_id_;
  std::unordered_map<unsigned, opStruct> opmap_;
};

}

#endif
