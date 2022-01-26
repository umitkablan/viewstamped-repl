#ifndef VSREPL_CLI_INCLUDED_
#define VSREPL_CLI_INCLUDED_ 1

#include "msgs.hpp"

namespace vsrepl
{

template <typename TMsgDispatcher>
class VSReplCli
{
public:
  VSReplCli(TMsgDispatcher& dp);

  void ConsumeCliMsg(int from, const MsgPersistedCliOp&);

  int ConsumeReply(int from, const MsgOpPersistedResponse&);

private:
  TMsgDispatcher& dispatcher_;
};

}

#endif
