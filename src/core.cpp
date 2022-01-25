#include "core.hpp"

#include "hasher.hpp"

#include <functional>
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
  , log_hash_(0)
  , prepare_sent_(false)
  , latest_healthtick_received_(1)
  , healthcheck_tick_(1)
  , trackDups_SVCs_(totreplicas)
  , trackDups_DVCs_(totreplicas)
  , trackDups_PrepResps_(totreplicas, -2)
  , trackDups_SVResps_(totreplicas, -2)
  , svResps_(totreplicas_)
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

  if (cnt > totreplicas_ / 2) { // include self...
    status_ = Status::Change;
    view_ = msgsvc.view;
    op_ = commit_;
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
  auto [isdup, idx] = checkDuplicate(trackDups_DVCs_, from, dvc.view);
  if (isdup)
    return 0;
  // cout << replica_ << ":" << view_ << " (DoVC) v:" << dvc.view << endl;

  auto cnt = std::count(
    trackDups_DVCs_.recv_replicas_.begin()+(idx*totreplicas_),
    trackDups_DVCs_.recv_replicas_.begin()+(idx+1)*totreplicas_,
    1);

  if (cnt <= totreplicas_ / 2) { // include self
    return 0;
  }

  cout << replica_ << ":" << view_ << "<-" << from << " (DoVC) consensus[" << cnt << "] for v:" << dvc.view << endl;
  view_ = dvc.view;
  op_ = commit_;
  // status_ = Status::Normal;

  healthcheck_tick_ = latest_healthtick_received_;
  for (int i = 0; i < totreplicas_; ++i) {
    if (i != replica_)
      dispatcher_.SendMsg(i, MsgStartView { dvc.view, commit_ });
  }

  return 0;
}

template <typename TMsgDispatcher, typename TStateMachine>
MsgStartViewResponse
ViewstampedReplicationEngine<TMsgDispatcher, TStateMachine>::ConsumeMsg(
    int from, const MsgStartView& sv)
{
  if (view_ < sv.view) {
    cout << replica_ << ":" << view_ << " (SV) my view is smaller than received v:" << sv.view << endl;
    op_ = commit_;
  }

  if (view_ <= sv.view) {
    if (view_ < sv.view)
      cout << replica_ << ":" << view_ << "<-" << from << " (SV) setting v:" << sv.view << endl;
    healthcheck_tick_ = latest_healthtick_received_;
    view_ = sv.view;
    status_ = Status::Normal;
  } else {
    cout << replica_ << ":" << view_ << " (SV) my view is bigger than received v:"
      << sv.view << "!! skipping..." << endl;
    return MsgStartViewResponse { "My view is bigger than received v:" + std::to_string(sv.view) };
  }

  std::vector<std::pair<int, MsgClientOp>> missing_logs;
  for (int i=logs_.size(); i-->0;)
    if (logs_[i].first > sv.last_commit)
      missing_logs.push_back(logs_[i]);
    else break;

  return MsgStartViewResponse { "", commit_, std::move(missing_logs) };
}

template <typename TMsgDispatcher, typename TStateMachine>
int ViewstampedReplicationEngine<TMsgDispatcher, TStateMachine>::ConsumeMsg(
    const MsgClientOp& msg)
{
  cout << replica_ << ":" << view_ << " (CliOp) " << msg.clientid << " msg.opstr:" << msg.toString()
       << " commit:" << op_ << "/" << commit_ << endl;
  if ((view_ % totreplicas_) != replica_) {
    dispatcher_.SendMsg(view_ % totreplicas_, msg);
    return 0;
  }

  if (op_ != commit_)
    return -1;
  ++op_;
  cliop_ = msg;
  latest_healthtick_received_ = healthcheck_tick_;
  prepare_sent_ = true;
  for (int i=0; i<totreplicas_; ++i)
    if (i != replica_)
      dispatcher_.SendMsg(i, MsgPrepare { view_, op_, commit_, log_hash_, msg });

  return 0;
}

template <typename TMsgDispatcher, typename TStateMachine>
MsgPrepareResponse
ViewstampedReplicationEngine<TMsgDispatcher, TStateMachine>::ConsumeMsg(
    int from, const MsgPrepare& msgpr)
{
  if ((view_ % totreplicas_) == replica_ && view_ == msgpr.view) {
    return MsgPrepareResponse { "I am not a follower!", msgpr.op };
  }

  // cout << replica_ << ":" << view_ << " (PREP) v:" << msgpr.view << " "
  //   << std::to_string(msgpr.op) + "/" << std::to_string(msgpr.commit) << endl;
  auto ret = MsgPrepareResponse { "", msgpr.op };
  if (view_ < msgpr.view) {
    cout << replica_ << ":" << view_ << "<-" << from << " (PREP) I am OUTDATED v:" << msgpr.view << endl;
    view_ = msgpr.view;
    status_ = Status::Normal;
    op_ = commit_;
  } else if (view_ > msgpr.view) {
    ret.err = "skipping old PREP v:" + std::to_string(msgpr.view) + " opstr:" + msgpr.cliop.opstr;
    return ret;
  }

  if (!(msgpr.commit == -1 && msgpr.op == -1 && msgpr.loghash == 1)
      && (commit_ > msgpr.commit || (commit_ == msgpr.commit && msgpr.loghash != log_hash_))) {
    cout << replica_ << ":" << view_ << "<-" << from << " (PREP) pop-back sz:" << logs_.size()
         << " commit:" << op_ << "/" << commit_
         << " msgpr.commit:" << msgpr.op << "/" << msgpr.commit
         << " msg.hash:" << msgpr.loghash << endl;
    logs_.pop_back();
    log_hash_ = mergeLogsHashes(logs_.begin(), logs_.end());
    commit_ = -1;
    if (!logs_.empty())
      commit_ = logs_.back().first;
    op_ = commit_;
  }

  healthcheck_tick_ = latest_healthtick_received_;
  if (msgpr.commit == op_) {
    if (op_ > commit_) {
      cout << replica_ << ":" << view_ << "<-" << from << " (PREP) committing op:" << op_
           << " cliop:" << cliop_.toString() << " sz:" << logs_.size() << endl;
      logs_.push_back(std::make_pair(op_, cliop_));
      commit_ = op_;
      log_hash_ = mergeLogsHashes(logs_.end() - 1, logs_.end(), log_hash_);
    }

    if (msgpr.op > commit_) {
      cliop_ = std::move(msgpr.cliop);
      op_ = msgpr.op;
    }

  } else if (commit_ < msgpr.commit) { // if (has_missing_logs_)
    ret.err = "My logs are not up-to-date " + std::to_string(msgpr.commit) + " >< "
              + std::to_string(op_) + "/" + std::to_string(commit_);
    dispatcher_.SendMsg(view_ % totreplicas_, MsgGetMissingLogs { view_, commit_ });
  }

  return ret;
}

template <typename TMsgDispatcher, typename TStateMachine>
int ViewstampedReplicationEngine<TMsgDispatcher, TStateMachine>::ConsumeReply(
    int from, const MsgStartViewResponse& svresp)
{
  if ((view_ % totreplicas_) != replica_) {
    cout << replica_ << ":" << view_ << "<-" << from << " (SVCResp) "
        << " lastcommit:" << svresp.last_commit << "; I am not the Leader " << endl;
    return -1;
  }
  auto [isdup, idx] = checkDuplicate(trackDups_SVResps_, from, svresp.last_commit);
  if (isdup)
    return 0; // double sent

  svResps_[from] = svresp;

  auto cnt = std::count(
    trackDups_SVResps_.recv_replicas_.begin()+(idx*totreplicas_),
    trackDups_SVResps_.recv_replicas_.begin()+(idx+1)*totreplicas_,
    1);

  cout << replica_ << ":" << view_ << "<-" << from << " (SVResp) consensus[" << cnt << "] "
       << op_ << "/" << commit_ << " lastcommit:" << svresp.last_commit << " missing.sz:"
       << svresp.missing_entries.size() << endl;
  if (cnt < totreplicas_ / 2) // is consensus not achieved?
    return 0;

  auto maxcommit = -2, maxidx = -1;
  for (int i = 0; i < totreplicas_; ++i) {
    if (trackDups_SVResps_.recv_replicas_[i + idx * totreplicas_]) {
      if (maxcommit < svResps_[i].last_commit) {
        maxcommit = svResps_[i].last_commit;
        maxidx = i;
      }
    }
  }

  clearDupsEntry(trackDups_SVResps_, idx);

  if (maxidx > -1) {
    auto& r = svResps_[maxidx];
    if (!r.missing_entries.empty())
      op_ = commit_ = r.missing_entries[0].first;
    const auto cursz = logs_.size();
    for (int i=r.missing_entries.size(); i-->0; ) {
      auto&& cliop = r.missing_entries[i];
      cout << replica_ << ":" << view_ << "<-" << from << " (SVResp) committing op:" << op_
           << " cliop: " << cliop_.toString() << " sz:" << logs_.size() << endl;
      logs_.push_back(std::move(cliop));
    }
    log_hash_ = mergeLogsHashes(logs_.begin() + cursz, logs_.end(), log_hash_);
  }
  status_ = Status::Normal;

  return 0;
}

template <typename TMsgDispatcher, typename TStateMachine>
int ViewstampedReplicationEngine<TMsgDispatcher, TStateMachine>::ConsumeReply(
    int from, const MsgPrepareResponse& presp)
{
  if (!presp.err.empty()) {
    cout << replica_ << ":" << view_ << "<-" << from << " (PrepResp) msg.err:" << presp.err << endl;
    return -2;
  }
  if ((view_ % totreplicas_) != replica_) {
    cout << replica_ << ":" << view_ << "<-" << from << " (PrepResp) I am NOT the Leader! msg.op:"
         << presp.op << endl;
    return -1;
  }
  if (op_ != presp.op) {
    cout << replica_ << ":" << view_ << "<-" << from << " (PrepResp) msg.op:" << presp.op
        << " does not match with my op:" << op_ << endl;
    return -3; // old view, unmatching
  }

  auto [isdup, idx] = checkDuplicate(trackDups_PrepResps_, from, presp.op);
  if (isdup)
    return 0; // double sent
  // cout << replica_ << ":" << view_ << "<-" << from << " (PrepResp) msg.op:"
  // << presp.op << " op_:" << op_ << endl;

  auto cnt = std::count(
    trackDups_PrepResps_.recv_replicas_.begin()+(idx*totreplicas_),
    trackDups_PrepResps_.recv_replicas_.begin()+(idx+1)*totreplicas_,
    1);

  if (cnt < totreplicas_ / 2) // is consensus not achieved?
    return 0;

  clearDupsEntry(trackDups_PrepResps_, idx);
  latest_healthtick_received_ = healthcheck_tick_;

  if (op_ == commit_) {
    return 0; // already committed
  }

  cout << replica_ << ":" << view_ << "<-" << from << " (PrepResp) committing consensus[" << cnt
    << "] op_:" << op_ << " cliop:" << cliop_.toString() << " sz:" << logs_.size() << endl;
  logs_.push_back(std::make_pair(op_, cliop_));
  commit_ = op_;
  log_hash_ = mergeLogsHashes(logs_.end() - 1, logs_.end(), log_hash_);

  return 0;
}

template <typename TMsgDispatcher, typename TStateMachine>
MsgMissingLogsResponse
ViewstampedReplicationEngine<TMsgDispatcher, TStateMachine>::ConsumeMsg(
    int from, const MsgGetMissingLogs& msgml)
{
  cout << replica_ << ":" << view_ << "<-" << from << " (GetML) msg.last_commit:"
    << msgml.my_last_commit << endl;
  MsgMissingLogsResponse ret{view_, "", std::make_pair(op_,cliop_)};
  if ((view_ % totreplicas_) != replica_) {
    ret.err = "I am not the leader " + std::to_string(replica_) + ":" + std::to_string(view_);
    return ret;
  }

  for (int i=logs_.size(); i-->0; ) {
    if (logs_[i].first > msgml.my_last_commit)
      ret.comitted_logs.push_back(logs_[i]);
    else break;
  }
  return ret;
}

template <typename TMsgDispatcher, typename TStateMachine>
int ViewstampedReplicationEngine<TMsgDispatcher, TStateMachine>::ConsumeReply(
    int from, const MsgMissingLogsResponse& mlresp)
{
  // cout << replica_ << ":" << view_ << "<-" << from << " (RespML) msg.last_commit:"
  //   << mlresp.my_last_commit << endl;
  if ((view_ % totreplicas_) == replica_) {
    cout << replica_ << ":" << view_ << "<-" << from << " (RespML) I am not a follower!" << endl;
    return -1;
  }
  if (from != (view_ % totreplicas_)) {
    cout << replica_ << ":" << view_ << "<-" << from << " (RespML) Source is not my leader" << endl;
    return -2;
  }

  auto cursz = logs_.size();
  for (int i=mlresp.comitted_logs.size(); i-->0; )
    logs_.push_back(mlresp.comitted_logs[i]);
  log_hash_ = mergeLogsHashes(logs_.begin() + cursz, logs_.end(), log_hash_);
  cout << replica_ << ":" << view_ << "<-" << from << " (RespML) commit_ to " << logs_.back().first
       << " op to " << mlresp.op_log.first << endl;
  commit_ = logs_.back().first;
  op_ = mlresp.op_log.first;
  cliop_ = std::move(mlresp.op_log.second);

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
        clearDupsEntry(trackDups_PrepResps_);
        return;
      }
    }
    for (int i = 0; i < totreplicas_; ++i) {
      if (i != replica_) {
        if (status_ == Status::Normal)
          dispatcher_.SendMsg(i, MsgPrepare { view_, commit_, commit_, log_hash_, MsgClientOp {} });
        else
          dispatcher_.SendMsg(i, MsgPrepare { view_, -1, -1, 1, MsgClientOp {} });
      }
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
                 "<-" + std::to_string(from) + " (checkDupSVC) view:" + std::to_string(view));
    td.recv_views_[emptyi] = view;
    td.recv_replicas_[emptyi * totreplicas_ + from] = 1;
    return std::make_pair(false, emptyi);
  }

  bool ret = td.recv_replicas_[viewi * totreplicas_ + from];
  td.recv_replicas_[viewi * totreplicas_ + from] = 1;
  return std::make_pair(ret, viewi);
}

template <typename TMsgDispatcher, typename TStateMachine>
void ViewstampedReplicationEngine<TMsgDispatcher, TStateMachine>::clearDupsEntry(trackDups& td, int idx)
{
  if (idx < 0) {
    std::fill(td.recv_replicas_.begin(), td.recv_replicas_.end(), 0);
    std::fill(td.recv_views_.begin(), td.recv_views_.end(), td.empty_id);
  } else {
    std::fill(td.recv_replicas_.begin()+(idx*totreplicas_),
        td.recv_replicas_.begin()+(idx+1)*totreplicas_,
        0);
    td.recv_views_[idx] = td.empty_id;
  }

}

}
