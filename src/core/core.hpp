#ifndef VSREPL_CORE_INCLUDED_
#define VSREPL_CORE_INCLUDED_ 1

#include "msgs.hpp"
#include "util.hpp"

#include <chrono>
#include <functional>
#include <optional>
#include <thread>
#include <unordered_set>
#include <variant>

namespace vsrepl {

enum class Status : char {
  Normal,
  Change,
};

template <typename TMsgDispatcher, typename TStateMachine>
class ViewstampedReplicationEngine
{
public:
  ViewstampedReplicationEngine(unsigned totreplicas, unsigned replica, TMsgDispatcher& dp, TStateMachine& sm,
    std::chrono::milliseconds tick_interval = std::chrono::milliseconds(150));

  void Start();
  void Stop();
  void ResetContent();

  std::variant<MsgLeaderRedirect, MsgPersistedCliOp, int>
    ConsumeMsg(const MsgClientOp&);
  int ConsumeMsg(int from, const MsgStartViewChange&);
  int ConsumeMsg(int from, const MsgDoViewChange&);
  MsgStartViewResponse ConsumeMsg(int from, const MsgStartView&);
  MsgPrepareResponse ConsumeMsg(int from, const MsgPrepare&);
  MsgMissingLogsResponse
    ConsumeMsg(int from, const MsgGetMissingLogs&);
  std::optional<MsgPersistedCliOp>
    ConsumeMsg(int from, const MsgOpPersistedQuery&);

  int ConsumeReply(int from, const MsgPrepareResponse&);
  int ConsumeReply(int from, const MsgStartViewResponse&);
  int ConsumeReply(int from, const MsgMissingLogsResponse&);

  int View() const { return view_; }
  Status GetStatus() const { return status_; }
  int CommitID() const { return commit_; }
  int OpID() const { return op_; }
  const std::vector<std::pair<int, MsgClientOp>>&
    GetCommittedLogs() const { return logs_; }
  std::size_t GetHash() const noexcept { return log_hash_; }

  void HealthTimeoutTicked();

private:
  TMsgDispatcher& dispatcher_;
  TStateMachine& state_machine_;
  std::chrono::milliseconds tick_interval_;

  const unsigned totreplicas_;
  const unsigned replica_;
  unsigned view_;
  Status status_;
  int op_;
  int commit_;
  std::size_t log_hash_;
  std::vector<std::pair<int, MsgClientOp>> logs_;
  std::unordered_set<std::pair<unsigned, uint64_t>, PairHasher> persisted_ops_;
  MsgClientOp cliop_;

  bool prepare_sent_;
  unsigned latest_healthtick_received_;
  unsigned healthcheck_tick_;

private:
  struct trackDups {
    trackDups(int totreplicas, int emptyID = -1)
      : recv_replicas_(totreplicas * totreplicas, 0)
      , recv_views_(totreplicas, emptyID)
      , empty_id (emptyID)
      {}
    std::vector<int8_t> recv_replicas_;
    std::vector<int> recv_views_;
    int empty_id;

    void Clear() {
      recv_replicas_.assign(recv_replicas_.size(), 0);
      recv_views_.assign(recv_views_.size(), empty_id);
    }
  };
  trackDups trackDups_SVCs_;
  trackDups trackDups_DVCs_;
  trackDups trackDups_PrepResps_;
  trackDups trackDups_SVResps_;
  std::vector<MsgStartViewResponse> svResps_;

  std::pair<bool,int>
  checkDuplicate(trackDups&, int from, int view);
  void clearDupsEntry(trackDups& td, int idx = -1);

private:
  void healthTickThread();

  volatile bool continue_healthtick_;
  std::thread healthTickThread_;
};

}
#endif
