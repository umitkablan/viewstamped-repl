#include "cli.hpp"

#include <iostream>
using std::cout;
using std::endl;

namespace vsrepl
{

template <typename TMsgDispatcher>
VSReplCli<TMsgDispatcher>::VSReplCli(unsigned client_id, TMsgDispatcher& dp, int totreplicas)
  : client_id_(client_id)
  , dispatcher_(dp)
  , totreplicas_(totreplicas)
  , consensus_min_(totreplicas/2)
  , last_view_(0)
  , last_op_id_(7)
{
}

template <typename TMsgDispatcher>
unsigned
VSReplCli<TMsgDispatcher>::InitOp(const std::string& opstr)
{
  auto ret = last_op_id_++;
  opmap_.insert(std::make_pair(ret, opStruct{opstr, OpState::DoesntExist}));
  return ret;
}

template <typename TMsgDispatcher>
typename VSReplCli<TMsgDispatcher>::OpState
VSReplCli<TMsgDispatcher>::StartOp(unsigned opID)
{
  const auto it = opmap_.find(opID);
  if (it == opmap_.end())
    return OpState::DoesntExist;
  if (it->second.st == OpState::DoesntExist) {
    dispatcher_.SendMsg(last_view_%totreplicas_, MsgClientOp{client_id_, it->second.str, it->first});
    it->second.st = OpState::Ongoing;
    return OpState::JustStarted;
  }
  return it->second.st;
}

template <typename TMsgDispatcher>
int VSReplCli<TMsgDispatcher>::DeleteOpID(unsigned opID)
{
  const auto it = opmap_.find(opID);
  if (it == opmap_.end()) 
    return -1;
  if (it->second.st != OpState::Consumed && it->second.st != OpState::DoesntExist)
    return -2;
  opmap_.erase(it);
  return 0;
}

template <typename TMsgDispatcher>
void VSReplCli<TMsgDispatcher>::ConsumeCliMsg(int from, const MsgPersistedCliOp& msgperscliop)
{
  setView(msgperscliop.view);
  const auto it = opmap_.find(msgperscliop.cliopid);
  if (it == opmap_.end())
    return;
  it->second.tot_received += 1;
  if (it->second.tot_received > consensus_min_)
    it->second.st = OpState::Consumed;
}

template <typename TMsgDispatcher>
int VSReplCli<TMsgDispatcher>::ConsumeReply(int from, const MsgOpPersistedResponse& msgoppersresp)
{
  // TODO:
  return 0;
}

template <typename TMsgDispatcher>
void VSReplCli<TMsgDispatcher>::ConsumeReply(int from, const MsgLeaderRedirect& msgleaderredir)
{
  setView(msgleaderredir.view);
  for (auto& p : opmap_)
    if (p.second.st == OpState::DoesntExist)
      dispatcher_.SendMsg(last_view_%totreplicas_, MsgClientOp{client_id_, p.second.str, p.first});
}

template <typename TMsgDispatcher>
void VSReplCli<TMsgDispatcher>::setView(int view)
{
  if (view <= last_view_)
    return;
  for (auto& p : opmap_)
    p.second.tot_received = 0;
  last_view_ = view;
}

}
