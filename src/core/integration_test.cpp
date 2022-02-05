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

using std::this_thread::sleep_for;

using engInFakeNetTyp = ViewstampedReplicationEngine<ParentMsgDispatcher, MockStateMachine>;
using cliInFakeNetTyp = VSReplCli<ParentMsgDispatcher>;
using fakeNetworkTyp = FakeTMsgBuggyNetwork<engInFakeNetTyp, cliInFakeNetTyp>;

using testMsgTyp = fakeNetworkTyp::TstMsgType;
using cliOpStatusTyp = cliInFakeNetTyp::OpState;


TEST(CoreWithBuggyNetwork, CoreEngine_Scenarios)
{
  const int clientMinIdx = 50;
  const bool shuffle_packets = true;

  fakeNetworkTyp buggynw(clientMinIdx,
    [](int from, int to, testMsgTyp, int vw) { return 0; }, shuffle_packets);
  std::vector<ParentMsgDispatcher> nwdispatchers {
    { 0, &buggynw }, { 1, &buggynw }, { 2, &buggynw }, { 3, &buggynw }, { 4, &buggynw },
    { clientMinIdx, &buggynw },
  };
  std::vector<MockStateMachine> statemachines(5);
  std::vector<engInFakeNetTyp> vsreps;
  vsreps.reserve(5); // we need explicit push_back due to copy constructor absence
  vsreps.push_back({ 5, 0, nwdispatchers[0], statemachines[0] });
  vsreps.push_back({ 5, 1, nwdispatchers[1], statemachines[1] });
  vsreps.push_back({ 5, 2, nwdispatchers[2], statemachines[2] });
  vsreps.push_back({ 5, 3, nwdispatchers[3], statemachines[3] });
  vsreps.push_back({ 5, 4, nwdispatchers[4], statemachines[4] });

  auto vsc0 = std::make_unique<cliInFakeNetTyp>(clientMinIdx, nwdispatchers[5], int(vsreps.size()));
  buggynw.SetEnginesStart(std::vector<engInFakeNetTyp*> {
    &vsreps[0], &vsreps[1], &vsreps[2], &vsreps[3], &vsreps[4] },
    std::vector<cliInFakeNetTyp*> { vsc0.get() });
  std::shared_ptr<void> buggynwDel(nullptr,
    [&buggynw](void*) { buggynw.CleanEnginesStop(); });

  for (int i = 0; i < 141; ++i) { // first command could take time due to leader's missing logs retrieval
    const auto v = vsreps[0].ConsumeMsg(MsgClientOp{clientMinIdx, "x=init_to0_v0-001", 86});
    if (std::holds_alternative<int>(v) && std::get<int>(v) == 0) break;
    sleep_for(std::chrono::milliseconds(50));
  }
  for (int i = 0; i < 41; ++i) {
    if (vsreps[0].CommitID() == 0
        // TODO: Normally we need only wait replica:0 CommitID but it has sporadic for now
        && vsreps[1].CommitID() == 0 && vsreps[2].CommitID() == 0
        && vsreps[3].CommitID() == 0 && vsreps[4].CommitID() == 0)
      break;
    ASSERT_LT(i, 40);
    sleep_for(std::chrono::milliseconds(50));
  }
  {
    int cnt = 0;
    for(int i=0; i<5; ++i)
      if (vsreps[i].OpID() == 0)
        ++cnt;
    ASSERT_GT(cnt, 2);
  }

  // --------------------------------------------------------------
  // make replica:0 isolated (receive & send) -> Changes to view:1 automatically
  // --------------------------------------------------------------
  cout << "*** make replica:0 isolated (receive & send) -> Changes to view:1 automatically" << endl;
  buggynw.SetDecideFun(
    [](int from, int to, testMsgTyp, int vw) -> int {
      if (from == to) return 0;
      return from==0 || to==0;
    });
  // We can only communicate with replica:0 directly (rather than using buggynw)
  vsreps[0].ConsumeMsg(MsgClientOp { 1212, "x=to0_isolated0_v0to1-1314", 287 });

  for (int i = 0; i < 100; ++i) {
    if (vsreps[1].View() > 0 && vsreps[1].GetStatus() == Status::Normal) break;
    sleep_for(std::chrono::milliseconds(50));
  }
  {
    auto cnt = 0;
    for (const auto& rep : vsreps) {
      if (rep.View() == 1 && rep.GetStatus() == Status::Normal)
        ++cnt;
    }
    ASSERT_THAT(cnt, ::testing::Gt(2));
  }
  // Op & CommitID is not 0+1 since replica:0 is isolated and cannot receive PrepareResponses
  ASSERT_EQ(0, vsreps[0].CommitID());
  ASSERT_EQ(1, vsreps[0].OpID());

  vsreps[0].ConsumeMsg(MsgClientOp{ clientMinIdx, "x=to0_isolated0_v1-1314", 88 });
  // re-join replica:0
  cout << "*** re-join replica:0" << endl;
  buggynw.SetDecideFun(
      [](int from, int to, testMsgTyp, int vw) { return 0; });
  for (int i = 0; i < 41; ++i) {
    if (vsreps[0].View() > 0 && vsreps[0].GetStatus() == Status::Normal) break;
    ASSERT_LT(i, 40);
    sleep_for(std::chrono::milliseconds(50));
  }
  ASSERT_EQ(1, vsreps[0].View());
  ASSERT_EQ(Status::Normal, vsreps[0].GetStatus());

  // --------------------------------------------------------------
  // make replica:1 isolated (receive-only, block outgoing messages)
  // --------------------------------------------------------------
  cout << "*** make replica:1 isolated (receive-only, block outgoing messages)" << endl;

  buggynw.SetDecideFun(
    [](int from, int to, testMsgTyp, int vw) -> int {
      if (from == to) return 0;
      return from==1;
    });
  vsreps[1].ConsumeMsg(MsgClientOp{ clientMinIdx, "x=to1_isolated1_v1to2-6655", 89 });
  for (int i = 0; i < 41; ++i) {
    if (vsreps[2].View() > 1 && vsreps[2].GetStatus() == Status::Normal) break;
    ASSERT_LT(i, 40);
    sleep_for(std::chrono::milliseconds(50));
  }
  {
    auto cnt = 0;
    for (const auto& rep : vsreps) {
      if (rep.View() == 2 && rep.GetStatus() == Status::Normal)
        ++cnt;
    }
    ASSERT_THAT(cnt, ::testing::Gt(2));
  }
  vsreps[2].ConsumeMsg(MsgClientOp{ clientMinIdx, "x=to2_isolated1_v2-6655", 90 });
  ASSERT_EQ(1, vsreps[2].OpID());
  ASSERT_EQ(0, vsreps[2].CommitID());
  for (int i = 0; i < 41; ++i) {
    if (1 == vsreps[2].CommitID()) break;
    ASSERT_LT(i, 40);
    sleep_for(std::chrono::milliseconds(50));
  }
  {
    const auto& logs = vsreps[2].GetCommittedLogs();
    ASSERT_EQ(logs.size(), 2);
    ASSERT_EQ(std::make_pair(0, MsgClientOp { clientMinIdx, "x=init_to0_v0-001", 86 }), logs[0]);
    ASSERT_EQ(std::make_pair(1, MsgClientOp { clientMinIdx, "x=to2_isolated1_v2-6655", 90 }), logs.back());
  }

  // Even replica:1 will adapt to new view since it can receive messages
  for (int i = 0; i < 41; ++i) {
    if (Status::Normal == vsreps[1].GetStatus() && 2 == vsreps[1].View()) break;
    ASSERT_LT(i, 40);
    sleep_for(std::chrono::milliseconds(50));
  }
  { // assert that replica:1 could also commit
    for (int i = 0; i < 41; ++i) {
      if (vsreps[1].CommitID() == 1) break;
      ASSERT_LT(i, 40);
      sleep_for(std::chrono::milliseconds(50));
    }
    ASSERT_EQ(vsreps[1].OpID(), 1);
    const auto& logs = vsreps[1].GetCommittedLogs();
    ASSERT_EQ(logs.size(), 2);
    ASSERT_EQ(std::make_pair(0, MsgClientOp{ clientMinIdx, "x=init_to0_v0-001", 86 }), logs[0]);
    ASSERT_EQ(std::make_pair(1, MsgClientOp{ clientMinIdx, "x=to2_isolated1_v2-6655", 90 }), logs.back());
  }

  // make replica:1 messages pass to destinations, again
  cout << "*** make replica:1 messages pass to destinations, again" << endl;
  buggynw.SetDecideFun(
      [](int from, int to, testMsgTyp, int vw) { return 0; });
  // since replica:1 was isolated receive-only, it should have correct view ASAP, no poll needed
  ASSERT_EQ(2, vsreps[1].View());
  ASSERT_EQ(Status::Normal, vsreps[1].GetStatus());
  {
    const auto& logs = vsreps[1].GetCommittedLogs();
    ASSERT_EQ(logs.size(), 2);
    ASSERT_EQ(std::make_pair(0, MsgClientOp{ clientMinIdx, "x=init_to0_v0-001", 86 }), logs[0]);
    ASSERT_EQ(std::make_pair(1, MsgClientOp{ clientMinIdx, "x=to2_isolated1_v2-6655", 90 }), logs.back());
  }

  // --------------------------------------------------------------
  // make replica:2-3 isolated (receive & send) -> Changes to view:4 automatically
  // --------------------------------------------------------------
  cout << "*** make replica:2-3 isolated (receive & send) -> Changes to view:4 automatically" << endl;
  buggynw.SetDecideFun(
    [](int from, int to, testMsgTyp, int vw) -> int {
      if (from == to) return 0;
      return from == 2 || to == 2 || from == 3 || to == 3;
    });
  for (int i = 0; i < 100; ++i) {
    if (vsreps[0].View() > 2 && vsreps[0].GetStatus() == Status::Normal
        && vsreps[1].View() > 2 && vsreps[1].GetStatus() == Status::Normal
        && vsreps[4].View() > 2 && vsreps[4].GetStatus() == Status::Normal)
      break;
    sleep_for(std::chrono::milliseconds(50));
  }
  {
    const auto& logs = vsreps[2].GetCommittedLogs();
    ASSERT_EQ(logs.size(), 2);
    ASSERT_EQ(std::make_pair(0, MsgClientOp{ clientMinIdx, "x=init_to0_v0-001", 86 }), logs[0]);
    ASSERT_EQ(std::make_pair(1, MsgClientOp{ clientMinIdx, "x=to2_isolated1_v2-6655", 90 }), logs.back());
  }
  for (int i = 0; i < 41; ++i) {
    if (vsreps[1].CommitID() > 0) break;
    ASSERT_LT(i, 40);
    sleep_for(std::chrono::milliseconds(50));
  }
  {
    const auto& logs = vsreps[1].GetCommittedLogs();
    ASSERT_EQ(logs.size(), 2);
    ASSERT_EQ(std::make_pair(0, MsgClientOp{ clientMinIdx, "x=init_to0_v0-001", 86 }), logs[0]);
    ASSERT_EQ(std::make_pair(1, MsgClientOp{ clientMinIdx, "x=to2_isolated1_v2-6655", 90 }), logs.back());
  }

  ASSERT_EQ(4, vsreps[0].View());
  ASSERT_EQ(Status::Normal, vsreps[0].GetStatus());
  ASSERT_EQ(4, vsreps[1].View());
  ASSERT_EQ(Status::Normal, vsreps[1].GetStatus());
  ASSERT_EQ(4, vsreps[4].View());
  ASSERT_EQ(Status::Normal, vsreps[4].GetStatus());

  // re-join replica:2-3
  cout << "*** re-join replica:2-3" << endl;
  buggynw.SetDecideFun(
      [](int from, int to, testMsgTyp, int vw) { return 0; });
  for (int i = 0; i < 20; ++i) {
    if (vsreps[2].View() > 3 && vsreps[2].GetStatus() == Status::Normal
        && vsreps[3].View() > 3 && vsreps[3].GetStatus() == Status::Normal)
      break;
    sleep_for(std::chrono::milliseconds(50));
  }
  ASSERT_EQ(4, vsreps[1].View());
  ASSERT_EQ(Status::Normal, vsreps[1].GetStatus());
  ASSERT_EQ(4, vsreps[2].View());
  ASSERT_EQ(Status::Normal, vsreps[2].GetStatus());

  // --------------------------------------------------------------
  // make replica:4-0 isolated: block send but not between 0 and 4
  // --------------------------------------------------------------
  cout << "*** make replica:4-0 isolated: block send but not between 0 and 4" << endl;
  buggynw.SetDecideFun(
      [](int from, int to, testMsgTyp, int vw) -> int {
        if (from == to) return 0;
        return (from == 4 && to != 0) || (from == 0 && to != 4); //from == 4 || from == 0;
      });
  vsreps[4].ConsumeMsg(MsgClientOp{ clientMinIdx, "xt=to4_isolated40_v4to6-001", 90 });
  for (int i = 0; i < 41; ++i) {
    if (vsreps[1].View() > 5 && vsreps[1].GetStatus() == Status::Normal) break;
    ASSERT_LT(i, 40);
    sleep_for(std::chrono::milliseconds(50));
  }
  {
    auto cnt = 0;
    for (const auto& rep : vsreps) {
      if (rep.View() == 6 && rep.GetStatus() == Status::Normal)
        ++cnt;
    }
    ASSERT_THAT(cnt, ::testing::Gt(2));
  }

  vsreps[4].ConsumeMsg(MsgClientOp{ clientMinIdx, "xt=to4_isolated40_v4to6-002", 91 });
  vsreps[1].ConsumeMsg(MsgClientOp{ clientMinIdx, "xu=to1_isolated40_v6-003", 92 });
  for (int i = 0; i < 41; ++i) {
    if (vsreps[1].CommitID() > 1) //vsreps[1].OpID() == vsreps[1].CommitID()
      break;
    ASSERT_LT(i, 40);
    sleep_for(std::chrono::milliseconds(50));
  }
  {
    const auto& logs = vsreps[1].GetCommittedLogs();
    ASSERT_GT(logs.size(), 2);
    ASSERT_EQ(std::make_pair(0, MsgClientOp{ clientMinIdx, "x=init_to0_v0-001", 86 }), logs[0]);
    ASSERT_EQ(std::make_pair(1, MsgClientOp{ clientMinIdx, "x=to2_isolated1_v2-6655", 90}), logs[1]);
    ASSERT_EQ(std::make_pair(2, MsgClientOp{ clientMinIdx, "xu=to1_isolated40_v6-003", 92 }), logs.back());
  }
  ASSERT_EQ(2, vsreps[1].CommitID());

  // re-join replica:4-0
  cout << "*** re-join replica:4-0" << endl;
  buggynw.SetDecideFun(
      [](int from, int to, testMsgTyp, int vw) { return 0; });
  vsreps[1].ConsumeMsg(MsgClientOp{ clientMinIdx, "xu=to1_joining40_isolating12_v6-0074", 93 });
  ASSERT_EQ(3, vsreps[1].OpID());
  ASSERT_EQ(2, vsreps[1].CommitID());
  for (int i = 0; i < 20; ++i) {
    if (vsreps[0].View() > 5 && vsreps[0].GetStatus() == Status::Normal
        && vsreps[4].View() > 5 && vsreps[4].GetStatus() == Status::Normal)
      break;
    sleep_for(std::chrono::milliseconds(50));
  }
  ASSERT_EQ(6, vsreps[0].View());
  ASSERT_EQ(Status::Normal, vsreps[0].GetStatus());
  ASSERT_EQ(6, vsreps[4].View());
  ASSERT_EQ(Status::Normal, vsreps[4].GetStatus());
  // Check new ops committed correctly
  for (int i = 0; i < 41; ++i) {
    // Checking OpID instead of CommitID would have sporadic behavior where the log could be
    //   discarded when it is not *yet _committed_ to the majority* - better be consistent
    if (vsreps[0].CommitID() == 3 && vsreps[4].CommitID() == 3) break;
    ASSERT_LT(i, 40);
    sleep_for(std::chrono::milliseconds(50));
  }

  //
  // SPLIT BRAIN --------------------------------------------------
  // --------------------------------------------------------------
  // make replica:1-2 an island of network separate from the rest
  // --------------------------------------------------------------
  //
  cout << "*** make replica:1-2 an island of network separate from the rest" << endl;

  buggynw.SetDecideFun(
    [](int from, int to, testMsgTyp, int vw) -> int {
      if (from == to) return 0;
      if ((from == 1 && to == 2) || (from == 2 && to == 1)) return 0;
      if ((from == 1 && to != 2) || (from == 2 && to != 1)) return 1;
      return (to == 2) || (to == 1);
    });
  for (int i = 0; i < 10; ++i) { // due to packet reorder, wait big group to initiate change
    if (vsreps[0].GetStatus() == Status::Change && vsreps[3].GetStatus() == Status::Change
        && vsreps[4].GetStatus() == Status::Change) break;
    // don't assert anything here, OK if all is not Status::Change at the same time
    sleep_for(std::chrono::milliseconds(100));
  }

  for (int i = 0; i < 41; ++i) {
    if (vsreps[3].View() > 6 && vsreps[3].GetStatus() == Status::Normal) break;
    ASSERT_LT(i, 40);
    sleep_for(std::chrono::milliseconds(50));
  }

  {
    auto cnt = 0;
    for (const auto& rep : vsreps) {
      if (rep.View() == 8 && rep.GetStatus() == Status::Normal)
        ++cnt;
    }
    ASSERT_THAT(cnt, ::testing::Gt(2));
  }

  ASSERT_EQ(6, vsreps[1].View());
  ASSERT_EQ(Status::Normal, vsreps[1].GetStatus());
  ASSERT_EQ(6, vsreps[2].View());
  ASSERT_EQ(Status::Normal, vsreps[2].GetStatus());

  // Separated leader should not be able to commit an op without consensus followers
  ASSERT_EQ(3, vsreps[2].OpID());
  ASSERT_EQ(3, vsreps[2].CommitID());
  vsreps[1].ConsumeMsg(MsgClientOp{ clientMinIdx, "x=to1_separated12_v6to8", 94 });
  ASSERT_EQ(4, vsreps[1].OpID());
  ASSERT_EQ(3, vsreps[1].CommitID());
  for (int i = 0; i < 41; ++i) {
    if (vsreps[2].OpID() == 4) break;
    ASSERT_LT(i, 40);
    sleep_for(std::chrono::milliseconds(50));
  }
  ASSERT_EQ(3, vsreps[2].CommitID());
  // Meanwhile the island of leader should be able to persist ops
  vsreps[1].ConsumeMsg(MsgClientOp{ clientMinIdx, "x=to1_isolated12_v6to8-1232", 95 });
  vsreps[3].ConsumeMsg(MsgClientOp{ clientMinIdx, "y=to3_isolated12_v8-1563", 96 });
  ASSERT_EQ(4, vsreps[3].OpID());
  ASSERT_EQ(3, vsreps[3].CommitID());
  for (int i = 0; i < 41; ++i) {
    if (4 == vsreps[3].CommitID()) break;
    ASSERT_LT(i, 40);
    sleep_for(std::chrono::milliseconds(50));
  }
  ASSERT_EQ(4, vsreps[3].OpID());
  ASSERT_EQ(4, vsreps[3].CommitID());
  ASSERT_EQ(4, vsreps[0].OpID());
  ASSERT_EQ(4, vsreps[4].OpID());
  // --------------------------------------------------------------
  // re-join replica:1-2 (join them to majority island)
  // --------------------------------------------------------------
  cout << "*** re-join replica:1-2 (join them to majority island)" << endl;
  buggynw.SetDecideFun(
      [](int from, int to, testMsgTyp, int vw) { return 0; });
  for (int i = 0; i < 20; ++i) {
    if (vsreps[1].View() > 6 && vsreps[1].GetStatus() == Status::Normal
        && vsreps[2].View() > 6 && vsreps[2].GetStatus() == Status::Normal)
      break;
    sleep_for(std::chrono::milliseconds(50));
  }
  ASSERT_EQ(8, vsreps[1].View());
  ASSERT_EQ(Status::Normal, vsreps[1].GetStatus());
  ASSERT_EQ(8, vsreps[2].View());
  ASSERT_EQ(Status::Normal, vsreps[2].GetStatus());

  // Check that re-joined island gets ops successfully
  for (int i = 0; i < 41; ++i) {
    if (vsreps[0].CommitID() == 4 && vsreps[1].CommitID() == 4 && vsreps[2].CommitID() == 4
        && vsreps[3].CommitID() == 4 && vsreps[4].CommitID() == 4)
      break;
    ASSERT_LT(i, 40);
    sleep_for(std::chrono::milliseconds(50));
  }

  {
    auto&& logs = vsreps[3].GetCommittedLogs();
    for (auto& p : logs)
      cout << "pp: " << p.first << " .. " << p.second.toString() << endl;
  }

  {
    auto&& logs = vsreps[1].GetCommittedLogs();
    ASSERT_EQ(5, logs.size());
    ASSERT_EQ(std::make_pair(0, MsgClientOp{ clientMinIdx, "x=init_to0_v0-001", 86 }), logs[0]);
    ASSERT_EQ(std::make_pair(1, MsgClientOp{ clientMinIdx, "x=to2_isolated1_v2-6655", 90 }), logs[1]);
    ASSERT_EQ(std::make_pair(2, MsgClientOp{ clientMinIdx, "xu=to1_isolated40_v6-003", 92 }), logs[2]);
    ASSERT_EQ(std::make_pair(3, MsgClientOp{ clientMinIdx, "xu=to1_joining40_isolating12_v6-0074", 93 }), logs[3]);
    ASSERT_EQ(std::make_pair(4, MsgClientOp{ clientMinIdx, "y=to3_isolated12_v8-1563", 96 }), logs[4]);
  }
  {
    auto&& logs = vsreps[0].GetCommittedLogs();
    ASSERT_EQ(5, logs.size());
    ASSERT_EQ(std::make_pair(0, MsgClientOp{ clientMinIdx, "x=init_to0_v0-001", 86 }), logs[0]);
    ASSERT_EQ(std::make_pair(1, MsgClientOp{ clientMinIdx, "x=to2_isolated1_v2-6655", 90 }), logs[1]);
    ASSERT_EQ(std::make_pair(2, MsgClientOp{ clientMinIdx, "xu=to1_isolated40_v6-003", 92 }), logs[2]);
    ASSERT_EQ(std::make_pair(3, MsgClientOp{ clientMinIdx, "xu=to1_joining40_isolating12_v6-0074", 93 }), logs[3]);
    ASSERT_EQ(std::make_pair(4, MsgClientOp{ clientMinIdx, "y=to3_isolated12_v8-1563", 96 }), logs[4]);
  }
  {
    auto&& logs = vsreps[3].GetCommittedLogs();
    ASSERT_EQ(5, logs.size());
    ASSERT_EQ(std::make_pair(0, MsgClientOp{ clientMinIdx, "x=init_to0_v0-001", 86 }), logs[0]);
    ASSERT_EQ(std::make_pair(1, MsgClientOp{ clientMinIdx, "x=to2_isolated1_v2-6655", 90 }), logs[1]);
    ASSERT_EQ(std::make_pair(2, MsgClientOp{ clientMinIdx, "xu=to1_isolated40_v6-003", 92 }), logs[2]);
    ASSERT_EQ(std::make_pair(3, MsgClientOp{ clientMinIdx, "xu=to1_joining40_isolating12_v6-0074", 93 }), logs[3]);
    ASSERT_EQ(std::make_pair(4, MsgClientOp{ clientMinIdx, "y=to3_isolated12_v8-1563", 96 }), logs[4]);
  }
  {
    auto&& logs = vsreps[4].GetCommittedLogs();
    ASSERT_EQ(5, logs.size());
    ASSERT_EQ(std::make_pair(0, MsgClientOp{ clientMinIdx, "x=init_to0_v0-001", 86 }), logs[0]);
    ASSERT_EQ(std::make_pair(1, MsgClientOp{ clientMinIdx, "x=to2_isolated1_v2-6655", 90 }), logs[1]);
    ASSERT_EQ(std::make_pair(2, MsgClientOp{ clientMinIdx, "xu=to1_isolated40_v6-003", 92 }), logs[2]);
    ASSERT_EQ(std::make_pair(3, MsgClientOp{ clientMinIdx, "xu=to1_joining40_isolating12_v6-0074", 93 }), logs[3]);
    ASSERT_EQ(std::make_pair(4, MsgClientOp{ clientMinIdx, "y=to3_isolated12_v8-1563", 96 }), logs[4]);
  }
}

TEST(ClientInBuggyNetwork, Client_Scenarios)
{
  const int clientMinIdx = 57;
  const auto shuffle_packets = true;

  fakeNetworkTyp buggynet(clientMinIdx,
    [](int from, int to, testMsgTyp, int view) { return 0; }, shuffle_packets);
  std::vector<ParentMsgDispatcher> disps {
    {0, &buggynet}, {1, &buggynet}, {2, &buggynet}, {3, &buggynet}, {4, &buggynet},
    {clientMinIdx, &buggynet}, {clientMinIdx+1, &buggynet},
  };
  std::vector<MockStateMachine> sms(5);
  std::vector<engInFakeNetTyp> vsreps;
  vsreps.reserve(7); // we need explicit push_back due to copy constructor absence
  vsreps.push_back({5, 0, disps[0], sms[0]});
  vsreps.push_back({5, 1, disps[1], sms[1]});
  vsreps.push_back({5, 2, disps[2], sms[2]});
  vsreps.push_back({5, 3, disps[3], sms[3]});
  vsreps.push_back({5, 4, disps[4], sms[4]});
  auto vsc0 = std::make_unique<cliInFakeNetTyp>(clientMinIdx,   disps[5], int(vsreps.size()));
  auto vsc1 = std::make_unique<cliInFakeNetTyp>(clientMinIdx+1, disps[6], int(vsreps.size()));
  buggynet.SetEnginesStart(
    std::vector<engInFakeNetTyp*>{ &vsreps[0], &vsreps[1], &vsreps[2], &vsreps[3], &vsreps[4] },
    std::vector<cliInFakeNetTyp*>{ vsc0.get(), vsc1.get() });
  std::shared_ptr<void> buggynwDel(nullptr,
    [&buggynet](void*) { buggynet.CleanEnginesStop(); });

  // --------------------------------------------------------------
  // default case, initial state: consume op using client
  // --------------------------------------------------------------
  const auto opid0 = vsc1->InitOp("cli1=opid0");
  auto st = vsc1->StartOp(opid0);
  ASSERT_EQ(cliOpStatusTyp::JustStarted, st);
  for (int i=0; i<141; ++i) { // first command could take time due to leader's missing logs retrieval
    ASSERT_NE(140, i);
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
    [](int from, int to, testMsgTyp, int vw) -> int {
      if (from == to) return 0;
      return from==0 || to==0;
    });
  {
    const auto opid1 = vsc0->InitOp("cli0=opid1");
    st = vsc0->StartOp(opid1);
    ASSERT_EQ(cliOpStatusTyp::JustStarted, st);
    for (int i = 0; i < 41; ++i) {
      if (vsc0->StartOp(opid1) == cliInFakeNetTyp::OpState::Consumed)
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
