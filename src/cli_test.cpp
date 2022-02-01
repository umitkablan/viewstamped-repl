#include "core_impl_test.cpp"

#include "hasher.hpp"

#include <array>
#include <gtest/gtest.h>

#include <iostream>
using std::cout;
using std::endl;

namespace vsrepl
{
namespace test
{

using ::testing::Eq;
using ::testing::A;
using ::testing::ElementsAre;
using ::testing::StrictMock;

using std::this_thread::sleep_for;
using VSRETestType = ViewstampedReplicationEngine<MockTMsgDispatcher, MockStateMachine>;

TEST(CliTest, ClientBasicStartDelete)
{
  using vsrCliTyp = VSReplCli<StrictMock<MockTMsgDispatcher>>;

  StrictMock<MockTMsgDispatcher> msgdispatcher;
  vsrCliTyp cli0(34, msgdispatcher, 5);

  const auto view = 0, leader = 0;
  std::vector<std::pair<int, MsgClientOp>> sent_cliops;
  EXPECT_CALL(msgdispatcher, SendMsg(A<int>(), A<const MsgClientOp&>())).WillRepeatedly([&sent_cliops](int to, const MsgClientOp& cliop) {
    sent_cliops.push_back(std::make_pair(to, cliop));
  });

  ASSERT_EQ(-1, cli0.DeleteOpID(112312)); // non-existing ID

  const auto opid0 = cli0.InitOp("op0");
  ASSERT_EQ(0, cli0.DeleteOpID(opid0));
  ASSERT_EQ(0, sent_cliops.size());

  ASSERT_EQ(vsrCliTyp::OpState::DoesntExist, cli0.StartOp(112312)); // non-existing ID

  const auto opid1 = cli0.InitOp("op1");
  auto res = cli0.StartOp(opid1);
  ASSERT_EQ(vsrCliTyp::OpState::JustStarted, res);
  ASSERT_EQ(1, sent_cliops.size());
  ASSERT_EQ(leader, sent_cliops[0].first);
  ASSERT_EQ(34, sent_cliops[0].second.clientid);
  ASSERT_EQ("op1", sent_cliops[0].second.opstr);
  ASSERT_EQ(opid1, sent_cliops[0].second.cliopid);
  sent_cliops.clear();
  ASSERT_EQ(-2, cli0.DeleteOpID(opid1));

  cli0.ConsumeCliMsg(leader, MsgPersistedCliOp{});
  cli0.ConsumeCliMsg(leader+1, MsgPersistedCliOp{});
  cli0.ConsumeCliMsg(leader+2, MsgPersistedCliOp{});
  ASSERT_EQ(-2, cli0.DeleteOpID(opid1));

  cli0.ConsumeCliMsg(leader, MsgPersistedCliOp{view, opid1});
  cli0.ConsumeCliMsg(leader+1, MsgPersistedCliOp{view, opid1});
  cli0.ConsumeCliMsg(leader+2, MsgPersistedCliOp{view, opid1});
  ASSERT_EQ(0, cli0.DeleteOpID(opid1));
}

TEST(CliTest, ClientBasicTimeout)
{
  using vsrCliTyp = VSReplCli<StrictMock<MockTMsgDispatcher>>;

  StrictMock<MockTMsgDispatcher> msgdispatcher;
  vsrCliTyp cli0(35, msgdispatcher, 5, 3);

  const auto view = 0, leader = 0;
  std::vector<std::pair<int, MsgClientOp>> sent_cliops;
  EXPECT_CALL(msgdispatcher, SendMsg(A<int>(), A<const MsgClientOp&>())).WillRepeatedly([&sent_cliops](int to, const MsgClientOp& cliop) {
    sent_cliops.push_back(std::make_pair(to, cliop));
  });

  const auto opid0 = cli0.InitOp("op0");
  auto res = cli0.StartOp(opid0);
  ASSERT_EQ(vsrCliTyp::OpState::JustStarted, res);
  ASSERT_EQ(1, sent_cliops.size());
  ASSERT_EQ(leader, sent_cliops[0].first);
  ASSERT_EQ(35, sent_cliops[0].second.clientid);
  ASSERT_EQ("op0", sent_cliops[0].second.opstr);
  ASSERT_EQ(opid0, sent_cliops[0].second.cliopid);
  sent_cliops.clear();

  cli0.TimeTick();
  cli0.ConsumeCliMsg(leader, MsgPersistedCliOp{view, opid0});
  cli0.TimeTick();
  cli0.ConsumeCliMsg(leader+1, MsgPersistedCliOp{view, opid0});
  cli0.TimeTick();
  ASSERT_EQ(1, sent_cliops.size());
  ASSERT_EQ(leader+1, sent_cliops[0].first);
  ASSERT_EQ(35, sent_cliops[0].second.clientid);
  ASSERT_EQ("op0", sent_cliops[0].second.opstr);
  ASSERT_EQ(opid0, sent_cliops[0].second.cliopid);
  sent_cliops.clear();

  cli0.ConsumeCliMsg(leader, MsgPersistedCliOp{view, opid0});
  cli0.ConsumeCliMsg(leader+1, MsgPersistedCliOp{view, opid0});
  cli0.TimeTick();
  cli0.TimeTick();
  cli0.TimeTick();
  ASSERT_EQ(1, sent_cliops.size());
  ASSERT_EQ(leader+2, sent_cliops[0].first);
  ASSERT_EQ(35, sent_cliops[0].second.clientid);
  ASSERT_EQ("op0", sent_cliops[0].second.opstr);
  ASSERT_EQ(opid0, sent_cliops[0].second.cliopid);
  sent_cliops.clear();

  res = cli0.StartOp(opid0);
  ASSERT_EQ(vsrCliTyp::OpState::Ongoing, res);

  cli0.ConsumeCliMsg(leader, MsgPersistedCliOp{view, opid0});
  cli0.ConsumeCliMsg(leader+1, MsgPersistedCliOp{view, opid0});
  cli0.ConsumeCliMsg(leader+2, MsgPersistedCliOp{view, opid0});
  res = cli0.StartOp(opid0);
  ASSERT_EQ(vsrCliTyp::OpState::Consumed, res);
  ASSERT_EQ(0, cli0.DeleteOpID(opid0));
}

TEST(VsReplClientInBuggyNetwork, Client_Scenarios)
{
  using vsreTyp = ViewstampedReplicationEngine<ParentMsgDispatcher, MockStateMachine>;
  using vsrCliTyp = VSReplCli<ParentMsgDispatcher>;
  using testMessageTyp = FakeTMsgBuggyNetwork<vsreTyp, vsrCliTyp>::TstMsgType;
  using cliOpStatusTyp = typename vsrCliTyp::OpState;
  const int clientMinIdx = 57;
  const auto shuffle_packets = true;

  FakeTMsgBuggyNetwork<vsreTyp, vsrCliTyp> buggynet(clientMinIdx,
    [](int from, int to, testMessageTyp, int view) {
      return 0;
    }, shuffle_packets);
  std::vector<ParentMsgDispatcher> disps {
    {0, &buggynet}, {1, &buggynet}, {2, &buggynet}, {3, &buggynet}, {4, &buggynet},
    {clientMinIdx, &buggynet}, {clientMinIdx+1, &buggynet},
  };
  std::vector<MockStateMachine> sms(5);
  std::vector<vsreTyp> vsreps;
  vsreps.reserve(7); // we need explicit push_back due to copy constructor absence
  vsreps.push_back({5, 0, disps[0], sms[0]});
  vsreps.push_back({5, 1, disps[1], sms[1]});
  vsreps.push_back({5, 2, disps[2], sms[2]});
  vsreps.push_back({5, 3, disps[3], sms[3]});
  vsreps.push_back({5, 4, disps[4], sms[4]});
  auto vsc0 = std::make_unique<vsrCliTyp>(clientMinIdx,   disps[5], int(vsreps.size()));
  auto vsc1 = std::make_unique<vsrCliTyp>(clientMinIdx+1, disps[6], int(vsreps.size()));
  buggynet.SetEnginesStart(
    std::vector<vsreTyp*>{ &vsreps[0], &vsreps[1], &vsreps[2], &vsreps[3], &vsreps[4] },
    std::vector<vsrCliTyp*>{ vsc0.get(), vsc1.get() });
  std::shared_ptr<void> buggynwDel(nullptr,
    [&buggynet](void*) { buggynet.CleanEnginesStop(); });

  // --------------------------------------------------------------
  // default case, initial state: consume op using client
  // --------------------------------------------------------------
  const auto opid0 = vsc1->InitOp("cli1=opid0");
  auto st = vsc1->StartOp(opid0);
  ASSERT_EQ(cliOpStatusTyp::JustStarted, st);
  for (int i=0; i<21; ++i) {
    ASSERT_NE(20, i);
    st = vsc1->StartOp(opid0);
    if (st == cliOpStatusTyp::Consumed) break;
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
  }
  auto res = vsc1->DeleteOpID(opid0);
  ASSERT_EQ(0, res);

  // --------------------------------------------------------------
  // isolate replica:0, the default leader
  // --------------------------------------------------------------
  cout << "*** isolate replica:0, the default leader" << endl;
  buggynet.SetDecideFun(
    [](int from, int to, testMessageTyp, int vw) -> int {
      if (from == to) return 0;
      return from==0 || to==0;
    });
  {
    const auto opid1 = vsc0->InitOp("cli0=opid1");
    st = vsc0->StartOp(opid1);
    ASSERT_EQ(cliOpStatusTyp::JustStarted, st);
    for (int i = 0; i < 41; ++i) {
      if (vsc0->StartOp(opid1) == vsrCliTyp::OpState::Consumed)
        break;
      ASSERT_LT(i, 40);
      sleep_for(std::chrono::milliseconds(100));
    }
    const auto res = vsc0->DeleteOpID(opid1);
    ASSERT_EQ(0, res);
  }
}

}
}
