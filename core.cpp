#include "core.hpp"

#include <iostream>
using std::cout;
using std::endl;

namespace vsrepl {

template <typename TMsgDispatcher, typename TStateMachine>
ViewstampedReplicationEngine<TMsgDispatcher, TStateMachine>::ViewstampedReplicationEngine(
  int totreplicas, int replica, TMsgDispatcher& dp, TStateMachine& sm,
  std::chrono::milliseconds tick_interval)
  : coresm_(totreplicas, replica)
  , dispatcher_(dp)
  , state_machine_(sm)
  , tick_interval_(tick_interval)
  , recv_prep_replies_(totreplicas, 0)
  , op_(-1)
  , commit_(-1)
  , prepare_sent_(false)
  , continue_healthtick_(true)
{
}

template <typename TMsgDispatcher, typename TStateMachine>
void ViewstampedReplicationEngine<TMsgDispatcher, TStateMachine>::Start()
{
  if (healthTickThread_.joinable())
    throw std::invalid_argument("core engine thread is running");
  continue_healthtick_ = true;
  healthTickThread_ = std::thread([this]() { healthTickThread(); });
}

template <typename TMsgDispatcher, typename TStateMachine>
void ViewstampedReplicationEngine<TMsgDispatcher, TStateMachine>::Stop()
{
  continue_healthtick_ = false;
  if (healthTickThread_.joinable())
    healthTickThread_.join();
}

template <typename TMsgDispatcher, typename TStateMachine>
int ViewstampedReplicationEngine<TMsgDispatcher, TStateMachine>::ConsumeMsg(int from, const MsgStartViewChange& msgsvc)
{
  auto dvcpp = coresm_.SVCReceived(from, msgsvc);
  if (dvcpp.index() == 1) {
    const auto& pp = std::get<std::pair<int, MsgDoViewChange>>(dvcpp);
    dispatcher_.SendMsg(pp.first, pp.second);
  } else if (dvcpp.index() == 2) {
    for (const auto& pp : std::get<VSREngineCore::SVCMsgsType>(dvcpp))
      dispatcher_.SendMsg(pp.first, pp.second);
  }
  return 0;
}

template <typename TMsgDispatcher, typename TStateMachine>
int ViewstampedReplicationEngine<TMsgDispatcher, TStateMachine>::ConsumeMsg(int from, const MsgDoViewChange& dvc)
{
  const auto svs = coresm_.DVCReceived(from, dvc);
  for (auto& sv : svs) // StartView
    dispatcher_.SendMsg(sv.first, sv.second);
  return 0;
}

template <typename TMsgDispatcher, typename TStateMachine>
MsgStartViewResponse ViewstampedReplicationEngine<TMsgDispatcher, TStateMachine>::ConsumeMsg(int from, const MsgStartView& sv)
{
  auto res = coresm_.SVReceived(from, sv);
  return MsgStartViewResponse { coresm_.View() };
}

template <typename TMsgDispatcher, typename TStateMachine>
int ViewstampedReplicationEngine<TMsgDispatcher, TStateMachine>::ConsumeMsg(const MsgClientOp& msg)
{
  // cout << coresm_.replica_ << ":" << coresm_.View() << " (CliOp) " << msg.clientid << " msg.opstr:" << msg.opstr << " op_:" << op_ << endl;
  if (coresm_.Leader() != coresm_.replica_) {
    dispatcher_.SendMsg(coresm_.Leader(), msg);
    return 0;
  }

  if (op_ != commit_)
    return -1;
  ++op_;
  prepare_sent_ = true;
  for (int i=0; i<coresm_.totreplicas_; ++i)
    if (i != coresm_.replica_) {
      dispatcher_.SendMsg(i, MsgPrepare { coresm_.View(), op_, commit_, msg.opstr });
    }

  return 0;
}

template <typename TMsgDispatcher, typename TStateMachine>
MsgPrepareResponse ViewstampedReplicationEngine<TMsgDispatcher, TStateMachine>::ConsumeMsg(int from, const MsgPrepare& msgpr)
{
  // cout << coresm_.replica_ << ":" << coresm_.View() << " (Prep) from:" << from << " prep:" << msgpr.commit << "/" << msgpr.op << " op_:" << op_ << endl;
  auto res = coresm_.PrepareReceived(msgpr);
  if (!res) {
    // Consume op
  }
  if (op_ < msgpr.op)
    op_ = msgpr.op;
  return MsgPrepareResponse { coresm_.View(), "", op_ };
}

template <typename TMsgDispatcher, typename TStateMachine>
int ViewstampedReplicationEngine<TMsgDispatcher, TStateMachine>::ConsumeReply(int from, const MsgStartViewResponse& svresp)
{
  return 0;
}

template <typename TMsgDispatcher, typename TStateMachine>
int ViewstampedReplicationEngine<TMsgDispatcher, TStateMachine>::ConsumeReply(int from, const MsgPrepareResponse& presp)
{
  // cout << coresm_.replica_ << ":" << coresm_.View() << " (PrepResp) from:" << from << " msg.op:" << presp.op << " op_:" << op_ << endl;
  if (!presp.err.empty())
    return -2;
  if (presp.view != coresm_.View())
    return -3;
  if (op_ == commit_)
    return 0; // already committed
  if (op_ != presp.op)
    return -1; // old view, unmatching
  if (recv_prep_replies_[from])
    return 0; // double sent

  recv_prep_replies_[from] = 1;
  if (std::count(recv_prep_replies_.begin(), recv_prep_replies_.end(), 1)
        >= (coresm_.totreplicas_ / 2)) {
    /*
     * Commit to state machine here
     */
    commit_ = op_;
    std::fill(recv_prep_replies_.begin(), recv_prep_replies_.end(), 0);
  }
  return 0;
}

template <typename TMsgDispatcher, typename TStateMachine>
void ViewstampedReplicationEngine<TMsgDispatcher, TStateMachine>::healthTickThread()
{
  while (continue_healthtick_) {
    auto res = coresm_.HealthTimeoutTicked(prepare_sent_);
    prepare_sent_ = false;
    if (res.index() == 1) {
      for (const auto& pp : std::get<VSREngineCore::PrepareMsgsType>(res))
        dispatcher_.SendMsg(pp.first, pp.second);
    } else if (res.index() == 2) {
      for (const auto& pp : std::get<VSREngineCore::SVCMsgsType>(res))
        dispatcher_.SendMsg(pp.first, pp.second);
    }
    std::this_thread::sleep_for(tick_interval_);
  }
}

}
