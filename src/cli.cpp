#include "cli.hpp"

#include <iostream>
using std::cout;
using std::endl;

namespace vsrepl
{

template <typename TMsgDispatcher>
VSReplCli<TMsgDispatcher>::VSReplCli(unsigned client_id, TMsgDispatcher& dp, int totreplicas,
    int timeout_tick, std::chrono::milliseconds tick_interval)
  : client_id_(client_id)
  , dispatcher_(dp)
  , totreplicas_(totreplicas)
  , consensus_min_(totreplicas/2)
  , last_view_(0)
  , last_op_id_(7)
  , tick_interval_(tick_interval)
  , timeout_tick_(timeout_tick)
{
}

template <typename TMsgDispatcher>
void VSReplCli<TMsgDispatcher>::Start()
{
  if (timeTickThread_.joinable())
    throw std::invalid_argument("core engine thread is running");
  continue_timetick_ = true;
  timeTickThread_ = std::thread([this]() { timeTickThread(); });
}

template <typename TMsgDispatcher>
void VSReplCli<TMsgDispatcher>::Stop()
{
  continue_timetick_ = false;
  if (timeTickThread_.joinable())
    timeTickThread_.join();
}

template <typename TMsgDispatcher>
unsigned
VSReplCli<TMsgDispatcher>::InitOp(const std::string& opstr)
{
  auto ret = last_op_id_++;
  opmap_.insert(std::make_pair(ret, opStruct{ opstr, OpState::DoesntExist, 0,
        last_view_ % totreplicas_ }));
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
  cout << last_view_ << ":" << client_id_ << " [MsgPersistedCliOp] view:" << msgperscliop.view
       << " cliopid:" << msgperscliop.cliopid << endl;
  if (!setView(msgperscliop.view))
    return;
  const auto it = opmap_.find(msgperscliop.cliopid);
  if (it == opmap_.end())
    return;
  it->second.recv_replicas_.insert(from);
  if (it->second.recv_replicas_.size() > consensus_min_)
    it->second.st = OpState::Consumed;
}

template <typename TMsgDispatcher>
void VSReplCli<TMsgDispatcher>::ConsumeReply(int from, const MsgLeaderRedirect& msgleaderredir)
{
  cout << last_view_ << ":" << client_id_ << " [MsgLeaderRedirect] view:" << msgleaderredir.view
    << " leader:" << msgleaderredir.leader << endl;
  if (!setView(msgleaderredir.view))
    return;
  for (auto& p : opmap_) {
    p.second.lastrep = -1;
    if (p.second.st != OpState::DoesntExist && p.second.st != OpState::Consumed)
      dispatcher_.SendMsg(last_view_%totreplicas_, MsgClientOp{client_id_, p.second.str, p.first});
  }
}

template <typename TMsgDispatcher>
void VSReplCli<TMsgDispatcher>::ConsumeReply(int from, const MsgPersistedCliOp& perscliop)
{
  if (!setView(perscliop.view))
    return;

  const auto it = opmap_.find(perscliop.cliopid);
  if (it == opmap_.end())
    return;
  if (it->second.st == OpState::Consumed)
    return;
  it->second.recv_replicas_.insert(from);
  if (it->second.recv_replicas_.size() > consensus_min_)
    it->second.st = OpState::Consumed;
}

template <typename TMsgDispatcher>
void VSReplCli<TMsgDispatcher>::TimeTick()
{
  for (auto& p : opmap_) {
    p.second.tick_cnt += 1;
    if (p.second.tick_cnt >= timeout_tick_) {
      if (p.second.lastrep == -1) p.second.lastrep = last_view_ % totreplicas_;
      else p.second.lastrep = (p.second.lastrep + 1) % totreplicas_;
      p.second.tick_cnt = 0;
      dispatcher_.SendMsg(p.second.lastrep, MsgClientOp{client_id_, p.second.str, p.first});
    }
  }
}

template <typename TMsgDispatcher>
bool VSReplCli<TMsgDispatcher>::setView(int view)
{
  if (view < last_view_)
    return false;
  if (view > last_view_) {
    for (auto& p : opmap_)
      p.second.recv_replicas_.clear();
    last_view_ = view;
  }
  return true;
}

template <typename TMsgDispatcher>
void VSReplCli<TMsgDispatcher>::timeTickThread()
{
  while (continue_timetick_) {
    TimeTick();
    std::this_thread::sleep_for(tick_interval_);
  }
}

}
