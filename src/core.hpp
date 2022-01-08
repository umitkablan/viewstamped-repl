#ifndef TSTAMPED_CORE_INCLUDED_
#define TSTAMPED_CORE_INCLUDED_ 1

#include "msgs.hpp"

#include <chrono>
#include <thread>

namespace vsrepl {

enum class Status : char {
  Normal,
  Change,
};


template <typename TMsgDispatcher, typename TStateMachine>
class ViewstampedReplicationEngine {
public:
    ViewstampedReplicationEngine(int totreplicas, int replica, TMsgDispatcher& dp, TStateMachine& sm,
        std::chrono::milliseconds tick_interval = std::chrono::milliseconds(150));

    void Start();
    void Stop();

    int ConsumeMsg(const MsgClientOp&);
    int ConsumeMsg(int from, const MsgStartViewChange&);
    int ConsumeMsg(int from, const MsgDoViewChange&);
    MsgStartViewResponse ConsumeMsg(int from, const MsgStartView&);
    MsgPrepareResponse ConsumeMsg(int from, const MsgPrepare&);
    MsgMissingLogsResponse
        ConsumeMsg(int from, const MsgGetMissingLogs&);

    int ConsumeReply(int from, const MsgPrepareResponse&);
    int ConsumeReply(int from, const MsgStartViewResponse&);
    int ConsumeReply(int from, const MsgMissingLogsResponse&);

    int View() const { return view_; }
    Status GetStatus() const { return status_; }
    int CommitID() const { return commit_; }
    int OpID() const { return op_; }

    void HealthTimeoutTicked();

private:
    TMsgDispatcher& dispatcher_;
    TStateMachine& state_machine_;
    std::chrono::milliseconds tick_interval_;

    const int totreplicas_;
    const int replica_;
    int view_;
    Status status_;
    int op_;
    int commit_;
    std::vector<std::pair<int,std::string>> logs_;
    std::string op_str_;

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
