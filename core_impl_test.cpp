#include "core.cpp"

#include "ifaces.hpp"

#include <vector>

#include <iostream>
using std::cout;
using std::endl;

#include <gmock/gmock.h>

namespace vsrepl {
namespace test {

class MockTMsgDispatcher : public IDispatcher {
public:
    MOCK_METHOD(void, SendMsg, (int to, const MsgClientOp&), (override));
    MOCK_METHOD(void, SendMsg, (int to, const MsgStartViewChange&), (override));
    MOCK_METHOD(void, SendMsg, (int to, const MsgDoViewChange&), (override));
    MOCK_METHOD(void, SendMsg, (int to, const MsgStartView&), (override));
    MOCK_METHOD(void, SendMsg, (int to, const MsgPrepare&), (override));
};

class MockStateMachine : public IStateMachine {
    MOCK_METHOD(int, Execute, (const std::string& opstr), (override));
};

class ParentMsgDispatcher : public IDispatcher {
    int from_;
    INetDispatcher* parent_;

public:
    ParentMsgDispatcher(int senderreplica, INetDispatcher* par)
        : from_(senderreplica)
        , parent_(par)
    {
    }

    void SendMsg(int to, const MsgClientOp& cliop) override
    {
        parent_->SendMsg(from_, to, cliop);
    }

    void SendMsg(int to, const MsgStartViewChange& svc) override
    {
        parent_->SendMsg(from_, to, svc);
    }

    void SendMsg(int to, const MsgDoViewChange& dvc) override
    {
        parent_->SendMsg(from_, to, dvc);
    }

    void SendMsg(int to, const MsgStartView& sv) override
    {
        parent_->SendMsg(from_, to, sv);
    }

    void SendMsg(int to, const MsgPrepare& pr) override
    {
        parent_->SendMsg(from_, to, pr);
    }

};
}

// initiate template for test integration
template class ViewstampedReplicationEngine<test::MockTMsgDispatcher, test::MockStateMachine>;
template class ViewstampedReplicationEngine<test::ParentMsgDispatcher, test::MockStateMachine>;

}
