#include "core.hpp"

#include <iostream>
using std::cout;
using std::endl;

namespace vsrepl {

template <typename TMsgDispatcher, typename TStateMachine>
ViewstampedReplicationEngine<TMsgDispatcher, TStateMachine>::ViewstampedReplicationEngine(
  int totreplicas, int replica, TMsgDispatcher& dp, TStateMachine& sm,
  std::chrono::milliseconds tick_interval)
  : dispatcher_(dp)
  , state_machine_(sm)
  , tick_interval_(tick_interval)
  , totreplicas_(totreplicas)
  , replica_(replica)
  , view_(0)
  , status_(Status::Normal)
  , op_(-1)
  , commit_(-1)
  , recv_prep_replies_(totreplicas, 0)
  , prepare_sent_(false)
  , latest_healthtick_received_(1)
  , healthcheck_tick_(1)
  , trackDups_SVCs_(totreplicas)
  , trackDups_DVCs_(totreplicas)
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
  auto [isdup, idx] = checkDuplicate(trackDups_SVCs_, from, msgsvc.view);
  if (isdup)
    return 0;
  // cout << replica_ << ":" << view_ << " (SVC) v:" << msgsvc.view << endl;

  auto cnt = std::count(
    trackDups_SVCs_.recv_replicas_.begin()+(idx*totreplicas_),
    trackDups_SVCs_.recv_replicas_.begin()+(idx+1)*totreplicas_,
    1);

  if (cnt > totreplicas_ / 2) { // don't include self
    status_ = Status::Change;
    view_ = msgsvc.view;
    healthcheck_tick_ = latest_healthtick_received_ - 1;
    dispatcher_.SendMsg(msgsvc.view % totreplicas_, MsgDoViewChange { msgsvc.view });
  } else if (msgsvc.view == view_ + 1
      && healthcheck_tick_ > latest_healthtick_received_
      && healthcheck_tick_ - latest_healthtick_received_ < 3) {
    for (int i = 0; i < totreplicas_; ++i)
      dispatcher_.SendMsg(i, MsgStartViewChange { view_ + 1 });
  }

  return 0;
}

template <typename TMsgDispatcher, typename TStateMachine>
int ViewstampedReplicationEngine<TMsgDispatcher, TStateMachine>::ConsumeMsg(int from, const MsgDoViewChange& dvc)
{
  auto [isdup, idx] = checkDuplicate(trackDups_DVCs_, from, dvc.view);
  if (isdup)
    return 0;
  // cout << replica_ << ":" << view_ << " (DoVC) v:" << dvc.view << endl;

  auto cnt = std::count(
    trackDups_DVCs_.recv_replicas_.begin()+(idx*totreplicas_),
    trackDups_DVCs_.recv_replicas_.begin()+(idx+1)*totreplicas_,
    1);

  if (cnt >= totreplicas_ / 2) { // include self
    view_ = dvc.view;
    status_ = Status::Normal;

    for (int i = 0; i < totreplicas_; ++i) {
      if (i != replica_)
        dispatcher_.SendMsg(i, MsgStartView { dvc.view });
    }
  }

  return 0;
}

template <typename TMsgDispatcher, typename TStateMachine>
MsgStartViewResponse ViewstampedReplicationEngine<TMsgDispatcher, TStateMachine>::ConsumeMsg(int from, const MsgStartView& sv)
{
  if (view_ < sv.view) {
    cout << replica_ << ":" << view_ << " (SV) my view is smaller than received v:" << sv.view << endl;
  }

  if (view_ <= sv.view) {
    healthcheck_tick_ = latest_healthtick_received_;
    view_ = sv.view;
    status_ = Status::Normal;
  } else {
    cout << replica_ << ":" << view_ << " (SV) my view is bigger than received v:" << sv.view << "!! skipping..." << endl;
    return MsgStartViewResponse { "My view is bigger than received v:" + std::to_string(sv.view) };
  }

  return MsgStartViewResponse {};
}

template <typename TMsgDispatcher, typename TStateMachine>
int ViewstampedReplicationEngine<TMsgDispatcher, TStateMachine>::ConsumeMsg(const MsgClientOp& msg)
{
  // cout << coresm_.replica_ << ":" << coresm_.View() << " (CliOp) " << msg.clientid << " msg.opstr:" << msg.opstr << " op_:" << op_ << endl;
  if ((view_ % totreplicas_) != replica_) {
    dispatcher_.SendMsg(view_ % totreplicas_, msg);
    return 0;
  }

  if (op_ != commit_)
    return -1;
  ++op_;
  prepare_sent_ = true;
  for (int i=0; i<totreplicas_; ++i)
    if (i != replica_)
      dispatcher_.SendMsg(i, MsgPrepare { view_, op_, commit_, msg.opstr });

  return 0;
}

template <typename TMsgDispatcher, typename TStateMachine>
MsgPrepareResponse ViewstampedReplicationEngine<TMsgDispatcher, TStateMachine>::ConsumeMsg(int from, const MsgPrepare& msgpr)
{
  // cout << replica_ << ":" << view_ << " (PREP) v:" << pr.view << endl;
  if (view_ < msgpr.view) {
    cout << replica_ << ":" << view_ << " (PREP) I am OUTDATED v:" << msgpr.view << endl;
    view_ = msgpr.view;
    status_ = Status::Normal;
  } else if (view_ > msgpr.view) {
    cout << replica_ << ":" << view_ << " (PREP) Skipping old v:" << msgpr.view << endl;
    return MsgPrepareResponse { "skipping old PREP", msgpr.op };
  }

  if (status_ == Status::Normal) {
    if (replica_ != view_ % totreplicas_)
      healthcheck_tick_ = latest_healthtick_received_;
  }
  op_ = msgpr.op;
  return MsgPrepareResponse { "", msgpr.op };
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
  if (op_ == commit_)
    return 0; // already committed
  if (op_ != presp.op)
    return -1; // old view, unmatching
  if (recv_prep_replies_[from])
    return 0; // double sent

  recv_prep_replies_[from] = 1;
  if (std::count(recv_prep_replies_.begin(), recv_prep_replies_.end(), 1)
        >= (totreplicas_ / 2)) {
    /*
     * Commit to state machine here
     */
    commit_ = op_;
    std::fill(recv_prep_replies_.begin(), recv_prep_replies_.end(), 0);
  }
  return 0;
}

template <typename TMsgDispatcher, typename TStateMachine>
void ViewstampedReplicationEngine<TMsgDispatcher, TStateMachine>::HealthTimeoutTicked()
{
  if (replica_ == (view_ % totreplicas_)) { // The leader
    if (prepare_sent_) {
      prepare_sent_ = false;
      return;
    }
    for (int i = 0; i < totreplicas_; ++i) {
      if (i != replica_)
        dispatcher_.SendMsg(i, MsgPrepare { view_, -1, -1, "" });
    }
    return;

  }
  // A follower

  ++healthcheck_tick_;
  auto diff = healthcheck_tick_ - latest_healthtick_received_;
  if (healthcheck_tick_ > latest_healthtick_received_ && diff > 2) {
    // cout << replica_ << ":" << view_ << " -> sensed isolated leader" << endl;
    if (diff < 4 || (diff > 5 && !(diff % 8))) {
      // cout << "#" << view_ << "\n";
      for (int i = 0; i < totreplicas_; ++i) {
          dispatcher_.SendMsg(i, MsgStartViewChange { view_ + 1 });
      }
    }
  }
}

template <typename TMsgDispatcher, typename TStateMachine>
void ViewstampedReplicationEngine<TMsgDispatcher, TStateMachine>::healthTickThread()
{
  while (continue_healthtick_) {
    HealthTimeoutTicked();
    std::this_thread::sleep_for(tick_interval_);
  }
}

template <typename TMsgDispatcher, typename TStateMachine>
std::pair<bool,int>
ViewstampedReplicationEngine<TMsgDispatcher, TStateMachine>::checkDuplicate(trackDups& td, int from, int view)
{
  auto find_from = [this, from, &td]() {
    for(int i=0; i<totreplicas_; ++i) {
      if (td.recv_replicas_[i * totreplicas_ + from])
        return i;
    }
    return -1;
  };
  auto find_view = [this, view, &td]() {
    std::pair<int,int> ret{-1,-1};
    for (int i = 0; i < totreplicas_; ++i) {
      if (td.recv_views_[i] == view) {
        ret.first = i;
        break;
      }
    }
    if (ret.first == -1)
      for(int i=0; i<totreplicas_; ++i)
        if (td.recv_views_[i] == -1) {
          ret.second = i;
          break;
        }
    return ret;
  };

  auto fromi = find_from();
  if (fromi != -1) {
    if (view == td.recv_views_[fromi])
      return std::make_pair(true, fromi);

      td.recv_replicas_[fromi * totreplicas_ + from] = 0; // clear previous view's recv record
      if (std::all_of(
        td.recv_replicas_.begin() + fromi*totreplicas_, td.recv_replicas_.begin() + (fromi+1)*totreplicas_,
        [](int v) { return v == 0; }))
        td.recv_views_[fromi] = -1;
  }

  auto [viewi, emptyi] = find_view();
  if (viewi == -1) {
    if (emptyi == -1) // impossible to not-find empty solution, just for completeness
      throw std::invalid_argument(std::to_string(replica_) + ":" + std::to_string(view_) + " (checkDupSVC) from:" + std::to_string(from) + " view:" + std::to_string(view));
    td.recv_views_[emptyi] = view;
    td.recv_replicas_[emptyi * totreplicas_ + from] = 1;
    return std::make_pair(false, emptyi);
  }

  bool ret = td.recv_replicas_[viewi * totreplicas_ + from];
  td.recv_replicas_[viewi * totreplicas_ + from] = 1;
  return std::make_pair(ret, viewi);
}

}
