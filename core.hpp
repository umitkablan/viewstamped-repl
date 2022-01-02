#ifndef TSTAMPED_CORE_INCLUDED_
#define TSTAMPED_CORE_INCLUDED_ 1

#include "coresm.hpp"

#include <chrono>
#include <thread>

namespace vsrepl {

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

    int ConsumeReply(int from, const MsgPrepareResponse&);
    int ConsumeReply(int from, const MsgStartViewResponse&);

    int View() const { return coresm_.View(); }
    Status GetStatus() const { return coresm_.GetStatus(); }
    int CommitID() const { return commit_; }
    int OpID() const { return op_; }

private:
    VSREngineCore coresm_;
    TMsgDispatcher& dispatcher_;
    TStateMachine& state_machine_;
    std::chrono::milliseconds tick_interval_;

    std::vector<int8_t> recv_prep_replies_;
    int op_;
    int commit_;

private:
    void healthTickThread();

    volatile bool continue_healthtick_;
    std::thread healthTickThread_;
};

}
#endif
