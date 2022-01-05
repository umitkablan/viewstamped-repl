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
  , prepare_sent_(false)
  , latest_healthtick_received_(1)
  , healthcheck_tick_(1)
  , trackDups_SVCs_(totreplicas)
  , trackDups_DVCs_(totreplicas)
  , trackDups_PrepResps_(totreplicas, -2)
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
int ViewstampedReplicationEngine<TMsgDispatcher, TStateMachine>::ConsumeMsg(
    int from, const MsgStartViewChange& msgsvc)
{
  auto [isdup, idx] = checkDuplicate(trackDups_SVCs_, from, msgsvc.view);
  if (isdup)
    return 0;
  // cout << replica_ << ":" << view_ << " (SVC) v:" << msgsvc.view << endl;

  auto cnt = std::count(
    trackDups_SVCs_.recv_replicas_.begin()+(idx*totreplicas_),
    trackDups_SVCs_.recv_replicas_.begin()+(idx+1)*totreplicas_,
    1);

  if (cnt > totreplicas_ / 2) { // don't include self...
    status_ = Status::Change;
    view_ = msgsvc.view;
    healthcheck_tick_ = latest_healthtick_received_;
    dispatcher_.SendMsg(msgsvc.view % totreplicas_, MsgDoViewChange { msgsvc.view });
  } else if (msgsvc.view == view_ + 1
      && healthcheck_tick_ > latest_healthtick_received_
      && healthcheck_tick_ - latest_healthtick_received_ < 3) {
    for (int i = 0; i < totreplicas_; ++i)
      dispatcher_.SendMsg(i, MsgStartViewChange { view_ + 1 }); //...since we send to ourself
  }

  return 0;
}

template <typename TMsgDispatcher, typename TStateMachine>
int ViewstampedReplicationEngine<TMsgDispatcher, TStateMachine>::ConsumeMsg(
    int from, const MsgDoViewChange& dvc)
{
  if ((view_ % totreplicas_) != replica_) {
    cout << replica_ << ":" << view_ << " (DoVC) v:" << dvc.view << "I am not the Leader" << endl;
    return -1;
  }

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

    healthcheck_tick_ = latest_healthtick_received_;
    for (int i = 0; i < totreplicas_; ++i) {
      if (i != replica_)
        dispatcher_.SendMsg(i, MsgStartView { dvc.view });
    }
  }

  return 0;
}

template <typename TMsgDispatcher, typename TStateMachine>
MsgStartViewResponse ViewstampedReplicationEngine<TMsgDispatcher, TStateMachine>::ConsumeMsg(
    int from, const MsgStartView& sv)
{
  if (view_ < sv.view) {
    cout << replica_ << ":" << view_ << " (SV) my view is smaller than received v:" << sv.view << endl;
  }

  if (view_ <= sv.view) {
    healthcheck_tick_ = latest_healthtick_received_;
    view_ = sv.view;
    status_ = Status::Normal;
  } else {
    cout << replica_ << ":" << view_ << " (SV) my view is bigger than received v:"
      << sv.view << "!! skipping..." << endl;
    return MsgStartViewResponse { "My view is bigger than received v:" + std::to_string(sv.view) };
  }

  return MsgStartViewResponse {};
}

template <typename TMsgDispatcher, typename TStateMachine>
int ViewstampedReplicationEngine<TMsgDispatcher, TStateMachine>::ConsumeMsg(
    const MsgClientOp& msg)
{
  cout << replica_ << ":" << view_ << " (CliOp) " << msg.clientid << " msg.opstr:" << msg.opstr
       << " commit:" << op_ << "/" << commit_ << endl;
  if ((view_ % totreplicas_) != replica_) {
    dispatcher_.SendMsg(view_ % totreplicas_, msg);
    return 0;
  }

  if (op_ != commit_)
    return -1;
  ++op_;
  latest_healthtick_received_ = healthcheck_tick_;
  prepare_sent_ = true;
  for (int i=0; i<totreplicas_; ++i)
    if (i != replica_)
      dispatcher_.SendMsg(i, MsgPrepare { view_, op_, commit_, msg.opstr });

  return 0;
}

template <typename TMsgDispatcher, typename TStateMachine>
MsgPrepareResponse ViewstampedReplicationEngine<TMsgDispatcher, TStateMachine>::ConsumeMsg(
    int from, const MsgPrepare& msgpr)
{
  if ((view_ % totreplicas_) == replica_ && view_ == msgpr.view) {
    return MsgPrepareResponse { "I am not a follower!", msgpr.op };
  }

  // cout << replica_ << ":" << view_ << " (PREP) v:" << pr.view << endl;
  auto ret = MsgPrepareResponse { "", msgpr.op };
  if (view_ < msgpr.view) {
    cout << replica_ << ":" << view_ << " (PREP) I am OUTDATED v:" << msgpr.view << endl;
    view_ = msgpr.view;
    status_ = Status::Normal;
  } else if (view_ > msgpr.view) {
    ret.err = "skipping old PREP v:" + std::to_string(msgpr.view);
  }

  if (status_ == Status::Normal) {
    if (replica_ != view_ % totreplicas_)
      healthcheck_tick_ = latest_healthtick_received_;
  }

  /*
   *
   * sync missing ops here
   *
   */
  commit_ = op_ = msgpr.op;

  return ret;
}

template <typename TMsgDispatcher, typename TStateMachine>
int ViewstampedReplicationEngine<TMsgDispatcher, TStateMachine>::ConsumeReply(
    int from, const MsgStartViewResponse& svresp)
{
  if ((view_ % totreplicas_) != replica_) {
    cout << replica_ << ":" << view_ << " (SVCResp) from:" << from
        << " lastcommit:" << svresp.last_commit << "; I am not the Leader " << endl;
    return -1;
  }

  return 0;
}

template <typename TMsgDispatcher, typename TStateMachine>
int ViewstampedReplicationEngine<TMsgDispatcher, TStateMachine>::ConsumeReply(
    int from, const MsgPrepareResponse& presp)
{
  if (!presp.err.empty()) {
    cout << replica_ << ":" << view_ << " (PrepResp) from:" << from << " msg.op:" << presp.op
         << " msg.err:" << presp.err << ((view_ % totreplicas_) == replica_ ? " [*]" : "")
         << endl;
    return -2;
  }
  if ((view_ % totreplicas_) != replica_) {
    cout << replica_ << ":" << view_ << " (PrepResp) from:" << from << " msg.op:" << presp.op
         << " I am NOT the Leader" << endl;
    return -1;
  }
  if (op_ != presp.op) {
    cout << replica_ << ":" << view_ << " (PrepResp) from:" << from << " msg.op:" << presp.op
        << " does not match with my op:" << op_ << endl;
    return -3; // old view, unmatching
  }

  auto [isdup, idx] = checkDuplicate(trackDups_PrepResps_, from, presp.op);
  if (isdup)
    return 0; // double sent
  // cout << replica_ << ":" << view_ << " (PrepResp) from:" << from << " msg.op:"
  // << presp.op << " op_:" << op_ << endl;

  auto cnt = std::count(
    trackDups_PrepResps_.recv_replicas_.begin()+(idx*totreplicas_),
    trackDups_PrepResps_.recv_replicas_.begin()+(idx+1)*totreplicas_,
    1);

  if (cnt < totreplicas_ / 2) // is consensus not achieved?
    return 0;

  std::fill(
      trackDups_PrepResps_.recv_replicas_.begin()+(idx*totreplicas_),
      trackDups_PrepResps_.recv_replicas_.begin()+(idx+1)*totreplicas_,
      0);
  trackDups_PrepResps_.recv_views_[idx] = -2;
  latest_healthtick_received_ = healthcheck_tick_;

  if (op_ == commit_) {
    return 0; // already committed
  }

  /*
   * Commit to state machine here
   */
  commit_ = op_;

  return 0;
}

template <typename TMsgDispatcher, typename TStateMachine>
void ViewstampedReplicationEngine<TMsgDispatcher, TStateMachine>::HealthTimeoutTicked()
{
  ++healthcheck_tick_;
  auto diff = healthcheck_tick_ - latest_healthtick_received_;

  if (replica_ == (view_ % totreplicas_)) { // The leader
    if (prepare_sent_) {
      prepare_sent_ = false;
      return;
    }
    if (op_ != commit_) {
      if (diff > 3) {
        cout << replica_ << ":" << view_ << " (TICK) reverting the op:" << op_
            << " to commit:" << commit_ << endl;
        op_ = commit_; // give up, revert the op
        return;
      }
    }
    for (int i = 0; i < totreplicas_; ++i) {
      if (i != replica_)
        dispatcher_.SendMsg(i, MsgPrepare { view_, op_, commit_, "" });
    }
    return;

  }
  // A follower

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
ViewstampedReplicationEngine<TMsgDispatcher, TStateMachine>::checkDuplicate(
    trackDups& td, int from, int view)
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
        if (td.recv_views_[i] == td.empty_id) {
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
            td.recv_replicas_.begin() + fromi*totreplicas_,
            td.recv_replicas_.begin() + (fromi+1)*totreplicas_,
            [](int v) { return v == 0; }))
        td.recv_views_[fromi] = td.empty_id;
  }

  auto [viewi, emptyi] = find_view();
  if (viewi == -1) {
    if (emptyi == -1) // impossible to not-find empty solution, just for completeness
      throw std::invalid_argument(std::to_string(replica_) + ":" + std::to_string(view_) +
                " (checkDupSVC) from:" + std::to_string(from) + " view:" + std::to_string(view));
    td.recv_views_[emptyi] = view;
    td.recv_replicas_[emptyi * totreplicas_ + from] = 1;
    return std::make_pair(false, emptyi);
  }

  bool ret = td.recv_replicas_[viewi * totreplicas_ + from];
  td.recv_replicas_[viewi * totreplicas_ + from] = 1;
  return std::make_pair(ret, viewi);
}

}
