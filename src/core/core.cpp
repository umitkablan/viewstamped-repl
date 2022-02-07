#include "core.hpp"

#include "hasher.hpp"
#include "util/logger.hpp"

#include <functional>

namespace vsrepl {

template <typename TMsgDispatcher, typename TStateMachine>
ViewstampedReplicationEngine<TMsgDispatcher, TStateMachine>::ViewstampedReplicationEngine(
  unsigned totreplicas, unsigned replica, TMsgDispatcher& dp, TStateMachine& sm,
  std::chrono::milliseconds tick_interval)
  : dispatcher_(dp)
  , state_machine_(sm)
  , tick_interval_(tick_interval)
  , totreplicas_(totreplicas)
  , replica_(replica)
  , view_(0)
  , status_(Status::Change)
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
  // log_info("{}:{} (SVC) v:{}", replica_, view_, msgsvc.view);

  auto cnt = std::count(
    trackDups_SVCs_.recv_replicas_.begin()+(idx*totreplicas_),
    trackDups_SVCs_.recv_replicas_.begin()+(idx+1)*totreplicas_,
    1);

  if (cnt > totreplicas_ / 2) { // include self...
    if (view_ < msgsvc.view) {
      log_info("{}:{}<-{} (SVC) consensus[{}] v:{}", replica_, view_, from, cnt, msgsvc.view);
      status_ = Status::Change;
      view_ = msgsvc.view;
      op_ = commit_;
    }
    if (view_ == msgsvc.view) {
      healthcheck_tick_ = latest_healthtick_received_;
      dispatcher_.SendMsg(msgsvc.view % totreplicas_, MsgDoViewChange { msgsvc.view });
    }
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
  // log_info("{}:{}<-{} (DoVC) v:{}", replica_, view_, from, dvc.view);

  auto cnt = std::count(
    trackDups_DVCs_.recv_replicas_.begin()+(idx*totreplicas_),
    trackDups_DVCs_.recv_replicas_.begin()+(idx+1)*totreplicas_,
    1);

  if (cnt <= totreplicas_ / 2) { // include self
    return 0;
  }

  log_info("{}:{}<-{} (DoVC) consensus[{}] for v:{}", replica_, view_, from, cnt, dvc.view);
  if (status_ == Status::Normal && view_ == dvc.view)
    return 0;

  view_ = dvc.view;
  op_ = commit_;
  status_ = Status::Change;

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
    log_info("{}:{}<-{} (SV) my view is smaller than received v:{}", replica_, view_, from, sv.view);
    op_ = commit_;
  }

  if (view_ <= sv.view) {
    if (view_ < sv.view)
      log_info("{}:{}<-{} (SV) setting v:{}", replica_, view_, from, sv.view);
    healthcheck_tick_ = latest_healthtick_received_;
    view_ = sv.view;
    status_ = Status::Normal;
  } else {
    log_info("{}:{}<-{} (SV) my view is bigger than received v:{}!! skipping...", replica_, view_, from, sv.view);
    return MsgStartViewResponse { view_, "My view is bigger than received v:" + std::to_string(sv.view) };
  }

  std::vector<std::pair<int, MsgClientOp>> missing_logs;
  for (int i=logs_.size(); i-->0;)
    if (logs_[i].first > sv.last_commit)
      missing_logs.push_back(logs_[i]);
    else break;

  return MsgStartViewResponse { view_, "", commit_, std::move(missing_logs) };
}

template <typename TMsgDispatcher, typename TStateMachine>
std::variant<MsgLeaderRedirect, MsgPersistedCliOp, int>
ViewstampedReplicationEngine<TMsgDispatcher, TStateMachine>::ConsumeMsg(
    const MsgClientOp& msg)
{
  log_info("{}:{} (CliOp) {} opstr:{} commit:{}/{}", replica_, view_, msg.clientid,
      msg.toString(), commit_, op_);
  std::variant<MsgLeaderRedirect, MsgPersistedCliOp, int> ret = 0;

  if (persisted_ops_.count(std::make_pair(msg.clientid, msg.cliopid))) {
    ret = MsgPersistedCliOp{view_, msg.cliopid};
    if (msg.dont_notify)
      return ret;
    auto mm = msg;
    mm.dont_notify = true;
    for (int i=0; i<totreplicas_; ++i)
      if (i != replica_) // not myself, I've already responded in ret
        dispatcher_.SendMsg(i, mm);
    return ret;
  }

  if ((view_ % totreplicas_) != replica_) {
    ret = MsgLeaderRedirect{view_, view_%totreplicas_};
    return ret;
  }

  if (op_ != commit_ || status_ != Status::Normal) { // not ready, retry
    ret = -1;
    return ret;
  }

  ++op_;
  cliop_ = msg;
  latest_healthtick_received_ = healthcheck_tick_;
  prepare_sent_ = true;
  for (int i=0; i<totreplicas_; ++i)
    if (i != replica_)
      dispatcher_.SendMsg(i, MsgPrepare { view_, op_, commit_, log_hash_, msg });
  return ret;
}

template <typename TMsgDispatcher, typename TStateMachine>
MsgPrepareResponse
ViewstampedReplicationEngine<TMsgDispatcher, TStateMachine>::ConsumeMsg(
    int from, const MsgPrepare& msgpr)
{
  if ((view_ % totreplicas_) == replica_ && view_ == msgpr.view) {
    return MsgPrepareResponse { "I am not a follower!", msgpr.op };
  }

  log_info("{}:{}<-{} (PREP) v:{} {}/{}", replica_, view_, from, msgpr.view, msgpr.commit, msgpr.op);
  //   << std::to_string(msgpr.op) + "/" << std::to_string(msgpr.commit) << endl;
  auto ret = MsgPrepareResponse { "", msgpr.op };
  if (view_ < msgpr.view) {
    log_info("{}:{}<-{} (PREP) I am OUTDATED v:{}", replica_, view_, from, msgpr.view);
    view_ = msgpr.view;
    status_ = Status::Normal;
    op_ = commit_;
  } else if (view_ > msgpr.view) {
    ret.err = "skipping old PREP v:" + std::to_string(msgpr.view) + " opstr:" + msgpr.cliop.opstr;
    return ret;
  }

  healthcheck_tick_ = latest_healthtick_received_;
  if (msgpr.commit == -1 && msgpr.op == -1 && msgpr.loghash == 1)
    return ret;

  if (commit_ > msgpr.commit || (commit_ == msgpr.commit && msgpr.loghash != log_hash_)) {
    log_info("{}:{}<-{} (PREP) pop-back sz:{} commit:{}/{} msgpr.commit:{}/{} msg.hash:{}",
        replica_, view_, from, logs_.size(), commit_, op_, msgpr.commit, msgpr.op, msgpr.loghash);
    logs_.pop_back();
    log_hash_ = mergeLogsHashes(logs_.begin(), logs_.end());
    commit_ = -1;
    if (!logs_.empty())
      commit_ = logs_.back().first;
    op_ = commit_;
  }

  if (msgpr.commit == op_) {
    if (op_ > commit_) {
      if (persisted_ops_.count(std::make_pair(cliop_.clientid, cliop_.cliopid)) == 0) {
        log_info("{}:{}<-{} (PREP) committing op:{} cliop:{} sz:{}", replica_, view_, from,
            op_, cliop_.toString(), logs_.size());
        logs_.push_back(std::make_pair(op_, cliop_));
        commit_ = op_;
        log_hash_ = mergeLogsHashes(logs_.end() - 1, logs_.end(), log_hash_);
        persisted_ops_.insert(std::make_pair(cliop_.clientid, cliop_.cliopid));
        dispatcher_.SendToClient(cliop_.clientid, MsgPersistedCliOp{view_, cliop_.cliopid});
      }
    }

    if (msgpr.op > commit_) {
      cliop_ = std::move(msgpr.cliop);
      op_ = msgpr.op;
    }

  } else if (commit_ < msgpr.commit || msgpr.commit != msgpr.op) {
    ret.err = "My logs are not up-to-date " + std::to_string(msgpr.commit) + " >< "
              + std::to_string(op_) + "/" + std::to_string(commit_);
    dispatcher_.SendMsg(view_ % totreplicas_, MsgGetMissingLogs { view_, commit_ });
  }

  ret.op = op_;
  return ret;
}

template <typename TMsgDispatcher, typename TStateMachine>
int ViewstampedReplicationEngine<TMsgDispatcher, TStateMachine>::ConsumeReply(
    int from, const MsgStartViewResponse& svresp)
{
  if ((view_ % totreplicas_) != replica_) {
    log_info("{}:{}<-{} (SVResp) lastcommit:{}; I am not the Leader!", replica_, view_, from,
        svresp.last_commit);
    return -1;
  }
  if (!svresp.err.empty()) {
    log_info("{}:{}<-{} (SVResp) err:{}", replica_, view_, from, svresp.err);
    return -2;
  }

  auto [isdup, idx] = checkDuplicate(trackDups_SVResps_, from, svresp.view);
  if (isdup)
    return 0; // double sent
  if (status_ == Status::Normal)
    return 0;

  svResps_[from] = svresp;

  auto cnt = std::count(
    trackDups_SVResps_.recv_replicas_.begin()+(idx*totreplicas_),
    trackDups_SVResps_.recv_replicas_.begin()+(idx+1)*totreplicas_,
    1);

  log_info("{}:{}<-{} (SVResp) consensus[{}] {}/{} lastcommit:{} missing.sz:{}", replica_, view_, from,
      cnt, commit_, op_, svresp.last_commit, svresp.missing_entries.size());
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
      log_info("{}:{}<-{} (SVResp) committing op:{} cliop:{} sz:{}", replica_, view_, from,
          op_, cliop_.toString(), logs_.size());
      logs_.push_back(cliop);
      persisted_ops_.insert(std::make_pair(cliop.second.clientid, cliop.second.cliopid));
      dispatcher_.SendToClient(cliop.second.clientid, MsgPersistedCliOp{view_, cliop.second.cliopid});
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
    log_info("{}:{}<-{} (PrepResp) msg.err:{}", replica_, view_, from, presp.err);
    return -2;
  }
  if ((view_ % totreplicas_) != replica_) {
    log_info("{}:{}<-{} (PrepResp) I am NOT the Leader! msg.op:{}", replica_, view_, from, presp.op);
    return -1;
  }
  if (op_ != presp.op) { // old view, unmatching
    if (presp.op != -1) {
      log_info("{}:{}<-{} (PrepResp) msg.op:{} does not match with my op:{}",
          replica_, view_, from, presp.op, op_);
      return -3;
    }
    return 0;
  }

  auto [isdup, idx] = checkDuplicate(trackDups_PrepResps_, from, presp.op);
  if (isdup)
    return 0; // double sent
  // log_info("{}:{}<-{} (PrepResp) msg.op:{} op_:{}", replica_, view_, from, presp.op, op_);

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

  log_info("{}:{}<-{} (PrepResp) committing consensus[{}] op_:{} cliop:{} sz:{}", replica_, view_, from,
      cnt, op_, cliop_.toString(), logs_.size());
  logs_.push_back(std::make_pair(op_, cliop_));
  commit_ = op_;
  log_hash_ = mergeLogsHashes(logs_.end() - 1, logs_.end(), log_hash_);
  persisted_ops_.insert(std::make_pair(cliop_.clientid, cliop_.cliopid));
  dispatcher_.SendToClient(cliop_.clientid, MsgPersistedCliOp{view_, cliop_.cliopid});

  return 0;
}

template <typename TMsgDispatcher, typename TStateMachine>
MsgMissingLogsResponse
ViewstampedReplicationEngine<TMsgDispatcher, TStateMachine>::ConsumeMsg(
    int from, const MsgGetMissingLogs& msgml)
{
  log_info("{}:{}<-{} (GetML) msg.last_commit:{}", replica_, view_, from, msgml.my_last_commit);
  MsgMissingLogsResponse ret{view_, "", std::make_pair(op_,cliop_), {}, log_hash_};
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
  // log_info("{}:{}<-{} (RespML) msg.last_commit:{}", replica_, view_, from, mlresp.my_last_commit);
  if ((view_ % totreplicas_) == replica_) {
    log_info("{}:{}<-{} (RespML) I am not a follower!", replica_, view_, from);
    return -1;
  }
  if (from != (view_ % totreplicas_)) {
    log_info("{}:{}<-{} (RespML) Source is not my leader", replica_, view_, from);
    return -2;
  }

  auto cursz = logs_.size();
  auto new_hash = log_hash_;
  for (auto it = mlresp.comitted_logs.rbegin(); it != mlresp.comitted_logs.rend(); ++it)
    new_hash = mergeLogsHashes(it.base()-1, it.base(), new_hash);
  if (new_hash != mlresp.tothash){
    log_info("{}:{}<-{} (RespML) our hash doesn't match new_hash:{} msg.tothash:{}",
        replica_, view_, from, new_hash, mlresp.tothash);
    return -3;
  }

  for (int i=mlresp.comitted_logs.size(); i-->0; ) {
    log_info("{}:{}<-{} (RespML) committing commit_id:{} cliop:{} sz:{}", replica_, view_, from,
        mlresp.comitted_logs[i].first, mlresp.comitted_logs[i].second.toString(), logs_.size());
    logs_.push_back(mlresp.comitted_logs[i]);
    const auto& msg = mlresp.comitted_logs[i].second;
    persisted_ops_.insert(std::make_pair(msg.clientid, msg.cliopid));
    dispatcher_.SendToClient(msg.clientid, MsgPersistedCliOp{view_, msg.cliopid});
  }
  log_hash_ = new_hash;
  log_info("{}:{}<-{} (RespML) commit_ to {} op to {}", replica_, view_, from,
      logs_.back().first, mlresp.op_log.first);
  commit_ = logs_.back().first;
  op_ = mlresp.op_log.first;
  cliop_ = std::move(mlresp.op_log.second);

  return 0;
}

template <typename TMsgDispatcher, typename TStateMachine>
std::optional<MsgPersistedCliOp>
ViewstampedReplicationEngine<TMsgDispatcher, TStateMachine>::ConsumeMsg(
  int from, const MsgOpPersistedQuery& opq)
{
  std::optional<MsgPersistedCliOp> ret;
  if (opq.perscliop.view == view_ &&
      persisted_ops_.count(std::make_pair(unsigned(from), opq.perscliop.cliopid))) {
    ret = MsgPersistedCliOp{view_, opq.perscliop.cliopid};
  }
  return ret;
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
    if (status_ == Status::Normal) {
      for (int i = 0; i < totreplicas_; ++i)
        if (i != replica_)
          dispatcher_.SendMsg(i, MsgPrepare { view_, commit_, op_, log_hash_, cliop_ });
    } else {
      for (int i = 0; i < totreplicas_; ++i)
        if (i != replica_)
          dispatcher_.SendMsg(i, MsgStartView{view_, commit_});
    }
    return;

  }
  // A follower

  if (healthcheck_tick_ > latest_healthtick_received_ && diff > 2) {
    // log_info("{}:{} sensed isolated leader", replica_, view_);
    if (diff < 4 || (diff > 5 && !(diff % 8))) {
      // log_info("{}:{} #{}", replica_, view_, view_);
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
