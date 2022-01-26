#include "cli.hpp"

namespace vsrepl
{

template <typename TMsgDispatcher>
VSReplCli<TMsgDispatcher>::VSReplCli(TMsgDispatcher& dp)
  : dispatcher_(dp)
{
}

template <typename TMsgDispatcher>
void VSReplCli<TMsgDispatcher>::ConsumeCliMsg(int from, const MsgPersistedCliOp& msgperscliop)
{
  // TODO:
}

template <typename TMsgDispatcher>
int VSReplCli<TMsgDispatcher>::ConsumeReply(int from, const MsgOpPersistedResponse& msgoppersresp)
{
  // TODO:
  return 0;
}

}
