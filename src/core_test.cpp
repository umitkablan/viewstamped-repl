#include "core_impl_test.cpp"

#include <array>
#include <gtest/gtest.h>

#include <iostream>
using std::cout;
using std::endl;

namespace vsrepl {
namespace test {

using ::testing::Eq;
using ::testing::A;
// using ::testing::TypedEq;
// using ::testing::_;
using ::testing::ElementsAre;
using ::testing::StrictMock;

using std::this_thread::sleep_for;
using VSRETestType = ViewstampedReplicationEngine<MockTMsgDispatcher, MockStateMachine>;

TEST(CoreTest, BasicDoViewChange)
{
  StrictMock<MockTMsgDispatcher> msgdispatcher;
  MockStateMachine sm;
  VSRETestType cr(3, 2, msgdispatcher, sm);
  for (int i=0; i<2; ++i) {
    cr.HealthTimeoutTicked();
  }
  {
    std::vector<int> res;
    EXPECT_CALL(msgdispatcher, SendMsg(A<int>(), A<const MsgStartViewChange&>())).WillRepeatedly([&res](int to, const MsgStartViewChange& svc) {
      ASSERT_EQ(1, svc.view);
      res.push_back(to);
    });

    cr.HealthTimeoutTicked();
    ASSERT_THAT(res, ElementsAre(0, 1, 2));
  }
  {
    cr.ConsumeMsg(1, MsgStartViewChange { 1 });

    EXPECT_CALL(msgdispatcher, SendMsg(1, A<const MsgDoViewChange&>())).WillOnce([](int to, const MsgDoViewChange& dvc) {
      ASSERT_EQ(1, dvc.view);
    });
    cr.ConsumeMsg(2, MsgStartViewChange { 1 });
  }
}

TEST(CoreTest, FilterDuplicateSVCs)
{
  StrictMock<MockTMsgDispatcher> msgdispatcher;
  MockStateMachine sm;
  VSRETestType cr(5, 4, msgdispatcher, sm);

  for (int i = 0; i < 2; ++i) {
    cr.HealthTimeoutTicked();
  }
  {
    std::vector<int> res;
    EXPECT_CALL(msgdispatcher, SendMsg(A<int>(), A<const MsgStartViewChange&>())).WillRepeatedly([&res](int to, const MsgStartViewChange& svc) {
      ASSERT_EQ(1, svc.view);
      res.push_back(to);
    });

    cr.HealthTimeoutTicked();
    ASSERT_THAT(res, ElementsAre(0, 1, 2, 3, 4));
  }
  {
    cr.ConsumeMsg(1, MsgStartViewChange { 1 });
    // Filter these and expect replica!=1 to send SVC to proceed to DVC
    cr.ConsumeMsg(1, MsgStartViewChange { 1 });
    cr.ConsumeMsg(1, MsgStartViewChange { 1 });
  }
  {
    cr.ConsumeMsg(4, MsgStartViewChange { 1 });

    EXPECT_CALL(msgdispatcher, SendMsg(1, A<const MsgDoViewChange&>())).WillOnce([](int to, const MsgDoViewChange& dvc) {
      ASSERT_EQ(1, dvc.view);
    });
    cr.ConsumeMsg(2, MsgStartViewChange { 1 });
  }
}

TEST(CoreTest, FilterDuplicateSVCsWhileViewInc)
{
  StrictMock<MockTMsgDispatcher> msgdispatcher;
  MockStateMachine sm;
  VSRETestType cr(5, 4, msgdispatcher, sm);

  for (int i = 0; i < 2; ++i) {
    cr.HealthTimeoutTicked();
  }
  {
    std::vector<int> res;
    EXPECT_CALL(msgdispatcher, SendMsg(A<int>(), A<const MsgStartViewChange&>())).WillRepeatedly([&res](int to, const MsgStartViewChange& svc) {
      ASSERT_EQ(1, svc.view);
      res.push_back(to);
    });

    cr.HealthTimeoutTicked();
    ASSERT_THAT(res, ElementsAre(0, 1, 2, 3, 4));
  }
  {
    cr.ConsumeMsg(1, MsgStartViewChange { 1 });
    // Filter these and expect replica!=1 to send SVC to proceed to DVC
    cr.ConsumeMsg(1, MsgStartViewChange { 1 });
    cr.ConsumeMsg(1, MsgStartViewChange { 1 });
  }
  {
    cr.ConsumeMsg(3, MsgStartViewChange { 2 });
    cr.ConsumeMsg(3, MsgStartViewChange { 2 });
    cr.ConsumeMsg(2, MsgStartViewChange { 2 });

    EXPECT_CALL(msgdispatcher, SendMsg(2, A<const MsgDoViewChange&>())).WillOnce([](int to, const MsgDoViewChange& dvc) {
      ASSERT_EQ(2, dvc.view);
    });
    cr.ConsumeMsg(4, MsgStartViewChange { 2 });
  }
}

TEST(CoreTest, DVCWhenOthersRecognizeLeaderDead)
{
  StrictMock<MockTMsgDispatcher> msgdispatcher;
  MockStateMachine sm;
  VSRETestType cr(5, 4, msgdispatcher, sm);

  // I got one unmet tick, not enough to emit SVC
  {
    cr.HealthTimeoutTicked();
  }
  // However someone else noticed leader inactivity in the mean time; join the party
  {
    std::vector<int> res;
    EXPECT_CALL(msgdispatcher, SendMsg(A<int>(), A<const MsgStartViewChange&>())).WillRepeatedly([&res](int to, const MsgStartViewChange& svc) {
      ASSERT_EQ(1, svc.view);
      res.push_back(to);
    });

    cr.ConsumeMsg(2, MsgStartViewChange { 1 });
    ASSERT_THAT(res, ElementsAre(0, 1, 2, 3, 4));
  }
  // And then, received another unmet tick
  {
    cr.HealthTimeoutTicked();

    std::vector<int> res;
    EXPECT_CALL(msgdispatcher, SendMsg(A<int>(), A<const MsgStartViewChange&>())).WillRepeatedly([&res](int to, const MsgStartViewChange& svc) {
      ASSERT_EQ(1, svc.view);
      res.push_back(to);
    });

    cr.HealthTimeoutTicked();
    ASSERT_THAT(res, ElementsAre(0, 1, 2, 3, 4));
  }
}

TEST(CoreTest, LeaderSendsPrepare)
{
  StrictMock<MockTMsgDispatcher> msgdispatcher;
  MockStateMachine sm;
  VSRETestType cr(5, 0, msgdispatcher, sm); // 0 is leader by default ,at the beginning

  std::vector<int> res;
  EXPECT_CALL(msgdispatcher, SendMsg(A<int>(), A<const MsgPrepare&>())).WillRepeatedly(
    [&res, &cr](int to, const MsgPrepare& pr) {
      ASSERT_EQ(0, pr.view);
      res.push_back(to);
      cr.ConsumeReply(to, MsgPrepareResponse { "", pr.op });
    });

  cr.HealthTimeoutTicked();
  ASSERT_THAT(res, ElementsAre(1, 2, 3, 4));
  res.clear();

  { // When ClientOp is received before Tick we optimize Prepare's
    for (int i = 0; i < 20; ++i) {
      cr.ConsumeMsg(MsgClientOp{1231, "x=y"});
      ASSERT_THAT(res, ElementsAre(1, 2, 3, 4));
      res.clear();
      cr.HealthTimeoutTicked();
      ASSERT_EQ(0, res.size()); // no prepares sent (for now)
    }

    cr.HealthTimeoutTicked(); // no optimization here since after ClientOp, ticked twice
    ASSERT_THAT(res, ElementsAre(1, 2, 3, 4));
  }
}

TEST(CoreTest, LeaderPrepareTimeouts)
{
  StrictMock<MockTMsgDispatcher> msgdispatcher;
  MockStateMachine sm;
  VSRETestType cr(5, 0, msgdispatcher, sm); // 0 is leader by default, at the beginning

  std::vector<std::pair<int, MsgPrepare>> recv;
  EXPECT_CALL(msgdispatcher, SendMsg(A<int>(), A<const MsgPrepare&>())).WillRepeatedly(
    [&recv, &cr](int to, const MsgPrepare& pr) {
      ASSERT_EQ(0, pr.view);
      recv.push_back(std::make_pair(to, pr));
      // don't call PrepareResponse to simulate a fail/isolation
    });

  {
    cr.ConsumeMsg(MsgClientOp { 1278, "xy=ert" });
    ASSERT_EQ(4, recv.size());
    for (int i = 0; i < recv.size(); ++i) {
      ASSERT_EQ(i+1, recv[i].first);
      ASSERT_EQ(0, recv[i].second.op);
    }
    recv.clear();
  }

  cr.ConsumeReply(1, MsgPrepareResponse { "", 0 }); // only replica:1 replies

  { // When ClientOp is received before Tick we optimize Prepare's
    cr.HealthTimeoutTicked();
    ASSERT_EQ(0, recv.size());
  }
  for (int i = 0; i < 2; ++i) { // let's timeout the op
    cr.HealthTimeoutTicked();
    ASSERT_EQ(4, recv.size());
    for (int i = 0; i < recv.size(); ++i) {
      ASSERT_EQ(i+1, recv[i].first);
      ASSERT_EQ(-1, recv[i].second.op);
    }
    recv.clear();
  }

  // Prepare timeouts and op is discarded
  {
    cr.HealthTimeoutTicked();
    ASSERT_EQ(0, recv.size());
    ASSERT_EQ(-1, cr.CommitID());
    ASSERT_EQ(-1, cr.OpID());
  }

  //
  // Successful case with challenges
  //

  {
    cr.ConsumeMsg(MsgClientOp { 1278, "xy=ert" });
    ASSERT_EQ(4, recv.size());
    for (int i = 0; i < recv.size(); ++i) {
      ASSERT_EQ(i+1, recv[i].first);
      ASSERT_EQ(0, recv[i].second.op);
    }
    recv.clear();
  }

  cr.ConsumeReply(2, MsgPrepareResponse { "", 0 }); // replica:2 replies
  ASSERT_EQ(-1, cr.CommitID());
  ASSERT_EQ(0, cr.OpID());

  { // When ClientOp is received before Tick we optimize Prepare's
    cr.HealthTimeoutTicked();
    ASSERT_EQ(0, recv.size());
    ASSERT_EQ(-1, cr.CommitID());
  }

  cr.ConsumeReply(2, MsgPrepareResponse { "", 0 }); // replica:2 replies again
  ASSERT_EQ(-1, cr.CommitID());
  cr.ConsumeReply(1, MsgPrepareResponse { "", 0 }); // replica:1 replies
  ASSERT_EQ(0, cr.CommitID());
  ASSERT_EQ(0, cr.OpID());

  cr.ConsumeReply(3, MsgPrepareResponse { "", 0 });
  {
    cr.ConsumeMsg(MsgClientOp { 1278, "zz=ttt" });
    ASSERT_EQ(4, recv.size());
    for (int i = 0; i < recv.size(); ++i) {
      ASSERT_EQ(i+1, recv[i].first);
      ASSERT_EQ(1, recv[i].second.op);
    }
    recv.clear();
  }
  cr.ConsumeReply(2, MsgPrepareResponse { "", 1 });
  ASSERT_EQ(0, cr.CommitID());
  cr.ConsumeReply(3, MsgPrepareResponse { "", 1 });
  ASSERT_EQ(1, cr.CommitID());
}

TEST(CoreTest, MissingLogs)
{
  StrictMock<MockTMsgDispatcher> msgdispatcher;
  MockStateMachine sm;
  VSRETestType cr1(3, 1, msgdispatcher, sm); // 0 is leader by default, 1 will be follower

  const int view = 6, leader = 0;
  std::vector<std::pair<int, MsgGetMissingLogs>> missing_log_reqs;
  EXPECT_CALL(msgdispatcher, SendMsg(A<int>(), A<const MsgGetMissingLogs&>())).WillRepeatedly(
    [&missing_log_reqs, view](int to, const MsgGetMissingLogs& gml) {
      ASSERT_EQ(view, gml.view);
      missing_log_reqs.push_back(std::make_pair(to, gml));
    });

  {
    cr1.ConsumeMsg(leader, MsgPrepare { view, 0, -1, "xz=efr" });
    ASSERT_EQ(0, missing_log_reqs.size());
  }
  ASSERT_EQ(0, cr1.OpID());
  {
    cr1.ConsumeMsg(leader, MsgPrepare { view, 0, 0, "" });
    ASSERT_EQ(0, missing_log_reqs.size());
  }
  ASSERT_EQ(0, cr1.CommitID());
  ASSERT_EQ(0, cr1.OpID());

  {
    cr1.ConsumeMsg(leader, MsgPrepare { view, 4, 0, "xzz=efrs" });
    ASSERT_EQ(0, missing_log_reqs.size());
    cr1.ConsumeMsg(leader, MsgPrepare { view, 5, 4, "azx=342" });
    ASSERT_EQ(0, missing_log_reqs.size());
  }
  ASSERT_EQ(4, cr1.CommitID());
  ASSERT_EQ(5, cr1.OpID());

  {
    cr1.ConsumeMsg(leader, MsgPrepare { view, 7, 6, "xzz=efrs" });
    ASSERT_EQ(1, missing_log_reqs.size());
    ASSERT_EQ(leader, missing_log_reqs[0].first);
    ASSERT_EQ(4, missing_log_reqs[0].second.my_last_commit);
    cr1.ConsumeReply(leader, MsgMissingLogsResponse { "", {7, "ss=45"}, {{6, "ee=dd"}} });
  }
  cr1.HealthTimeoutTicked();
  cr1.HealthTimeoutTicked();
  ASSERT_EQ(6, cr1.CommitID());
  ASSERT_EQ(7, cr1.OpID());
}


TEST(CoreWithBuggyNetwork, ViewChange_BuggyNetworkNoShuffle_Scenarios)
{
  using VSREtype = ViewstampedReplicationEngine<ParentMsgDispatcher, MockStateMachine>;

  FakeTMsgBuggyNetwork<VSREtype> buggynw(
    [](int from, int to, FakeTMsgBuggyNetwork<VSREtype>::TstMsgType, int vw) {
      return 0;
    }, false);
  std::vector<ParentMsgDispatcher> nwdispatchers {
    { 0, &buggynw }, { 1, &buggynw }, { 2, &buggynw }, { 3, &buggynw }, { 4, &buggynw },
  };
  std::vector<MockStateMachine> statemachines(5);
  std::vector<VSREtype> vsreps;
  vsreps.reserve(5); // we need explicit push_back due to copy constructor absence
  vsreps.push_back({ 5, 0, nwdispatchers[0], statemachines[0] });
  vsreps.push_back({ 5, 1, nwdispatchers[1], statemachines[1] });
  vsreps.push_back({ 5, 2, nwdispatchers[2], statemachines[2] });
  vsreps.push_back({ 5, 3, nwdispatchers[3], statemachines[3] });
  vsreps.push_back({ 5, 4, nwdispatchers[4], statemachines[4] });
  buggynw.SetEnginesStart(std::vector<VSREtype*> {
    &vsreps[0], &vsreps[1], &vsreps[2], &vsreps[3], &vsreps[4] });
  std::shared_ptr<void> buggynwDel(nullptr,
    [&buggynw](void*) { buggynw.CleanEnginesStop(); });


  buggynw.SendMsg(-1, 2, MsgClientOp { 1212, "x=12" });
  for (int i = 0; i < 151; ++i) {
    if (vsreps[0].CommitID() == 0
        // TODO: Normally we need only wait replica:0 CommitID but it has sporadic for now
        && vsreps[1].CommitID() == 0 && vsreps[2].CommitID() == 0
        && vsreps[3].CommitID() == 0 && vsreps[4].CommitID() == 0)
      break;
    ASSERT_LT(i, 150);
    sleep_for(std::chrono::milliseconds(5));
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
  buggynw.SetDecideFun(
    [](int from, int to, FakeTMsgBuggyNetwork<VSREtype>::TstMsgType, int vw) -> int {
      if (from == to) return 0;
      return from==0 || to==0;
    });
  // We can only (and safely) communicate with replica:0 directly
  vsreps[0].ConsumeMsg(MsgClientOp { 1212, "x=13" });
  for (int i = 0; i < 100; ++i) {
    if (vsreps[1].View() > 0 && vsreps[1].GetStatus() == Status::Normal
        && vsreps[2].View() > 0 && vsreps[2].GetStatus() == Status::Normal
        && vsreps[3].View() > 0 && vsreps[3].GetStatus() == Status::Normal
        && vsreps[4].View() > 0 && vsreps[4].GetStatus() == Status::Normal)
      break;
    sleep_for(std::chrono::milliseconds(50));
  }
  // Op & CommitID is not 0+1 since replica:0 is isolated and cannot receive PrepareResponses
  ASSERT_EQ(0, vsreps[0].CommitID());
  ASSERT_EQ(0, vsreps[0].OpID());

  int cnt = 0;
  for (const auto& rep : vsreps) {
    if (rep.View() == 1 && rep.GetStatus() == Status::Normal)
      ++cnt;
  }
  ASSERT_THAT(cnt, ::testing::Gt(3));

  // Make replica:0 non-isolated again
  buggynw.SetDecideFun(
      [](int from, int to, FakeTMsgBuggyNetwork<VSREtype>::TstMsgType, int vw) { return 0; });
  for (int i = 0; i < 40; ++i) {
    if (vsreps[0].View() > 0 && vsreps[0].GetStatus() == Status::Normal) break;
    sleep_for(std::chrono::milliseconds(50));
  }
  ASSERT_EQ(1, vsreps[0].View());
  ASSERT_EQ(Status::Normal, vsreps[0].GetStatus());

  // --------------------------------------------------------------
  // make replica:1 isolated (receive-only, block outgoing messages)
  // --------------------------------------------------------------
  buggynw.SetDecideFun(
    [](int from, int to, FakeTMsgBuggyNetwork<VSREtype>::TstMsgType, int vw) -> int {
      if (from == to) return 0;
      return from==1;
    });
  for (int i = 0; i < 100; ++i) {
    if (vsreps[0].View() > 1 && vsreps[0].GetStatus() == Status::Normal
        && vsreps[2].View() > 1 && vsreps[2].GetStatus() == Status::Normal
        && vsreps[3].View() > 1 && vsreps[3].GetStatus() == Status::Normal
        && vsreps[4].View() > 1 && vsreps[4].GetStatus() == Status::Normal)
      break;
    sleep_for(std::chrono::milliseconds(50));
  }

  // Even replica:1 will adapt to new view since it can receive messages
  ASSERT_EQ(2, vsreps[1].View());
  ASSERT_EQ(Status::Normal, vsreps[1].GetStatus());
  cnt = 0;
  for (const auto& rep : vsreps) {
    if (rep.View() == 2 && rep.GetStatus() == Status::Normal)
      ++cnt;
  }
  ASSERT_THAT(cnt, ::testing::Gt(3));

  // make replica:1 messages pass to destinations, again
  buggynw.SetDecideFun(
      [](int from, int to, FakeTMsgBuggyNetwork<VSREtype>::TstMsgType, int vw) { return 0; });
  // since replica:1 was isolated receive-only, it should have correct view ASAP, no poll needed
  ASSERT_EQ(2, vsreps[1].View());
  ASSERT_EQ(Status::Normal, vsreps[1].GetStatus());

  // --------------------------------------------------------------
  // make replica:2-3 isolated (receive & send) -> Changes to view:4 automatically
  // --------------------------------------------------------------
  buggynw.SetDecideFun(
    [](int from, int to, FakeTMsgBuggyNetwork<VSREtype>::TstMsgType, int vw) -> int {
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

  ASSERT_EQ(4, vsreps[0].View());
  ASSERT_EQ(Status::Normal, vsreps[0].GetStatus());
  ASSERT_EQ(4, vsreps[1].View());
  ASSERT_EQ(Status::Normal, vsreps[1].GetStatus());
  ASSERT_EQ(4, vsreps[4].View());
  ASSERT_EQ(Status::Normal, vsreps[4].GetStatus());

  // Make replica:2-3 non-isolated again
  buggynw.SetDecideFun(
      [](int from, int to, FakeTMsgBuggyNetwork<VSREtype>::TstMsgType, int vw) { return 0; });
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
  buggynw.SetDecideFun(
      [](int from, int to, FakeTMsgBuggyNetwork<VSREtype>::TstMsgType, int vw) -> int {
        if (from == to) return 0;
        return (from == 4 && to != 0) || (from == 0 && to != 4); //from == 4 || from == 0;
      });
  vsreps[4].ConsumeMsg(MsgClientOp { 1688, "xt=55" });
  for (int i = 0; i < 100; ++i) {
    if (vsreps[1].View() > 5 && vsreps[1].GetStatus() == Status::Normal
        && vsreps[2].View() > 5 && vsreps[2].GetStatus() == Status::Normal
        && vsreps[3].View() > 5 && vsreps[3].GetStatus() == Status::Normal)
      break;
    sleep_for(std::chrono::milliseconds(50));
  }

  ASSERT_EQ(6, vsreps[1].View());
  ASSERT_EQ(Status::Normal, vsreps[1].GetStatus());
  ASSERT_EQ(6, vsreps[2].View());
  ASSERT_EQ(Status::Normal, vsreps[2].GetStatus());
  ASSERT_EQ(6, vsreps[3].View());
  ASSERT_EQ(Status::Normal, vsreps[3].GetStatus());

  vsreps[1].ConsumeMsg(MsgClientOp { 5908, "xu=75" });
  for (int i = 0; i < 21; ++i) {
    if (vsreps[1].OpID() == vsreps[1].CommitID())
      break;
    ASSERT_LT(i, 20);
    sleep_for(std::chrono::milliseconds(50));
  }
  ASSERT_EQ(1, vsreps[1].CommitID());

  // Make replica:4-0 non-isolated again
  buggynw.SetDecideFun(
      [](int from, int to, FakeTMsgBuggyNetwork<VSREtype>::TstMsgType, int vw) { return 0; });
  vsreps[1].ConsumeMsg(MsgClientOp { 5908, "xu=69" });
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
  // Check new ops propagated correctly
  for (int i = 0; i < 21; ++i) {
    if (vsreps[0].OpID() == 2 && vsreps[4].OpID() == 2)
      break;
    ASSERT_LT(i, 20);
    sleep_for(std::chrono::milliseconds(50));
  }

  //
  // SPLIT BRAIN --------------------------------------------------
  // --------------------------------------------------------------
  // make replica:1-2 an island of network separate from the rest
  // --------------------------------------------------------------
  //

  buggynw.SetDecideFun(
    [](int from, int to, FakeTMsgBuggyNetwork<VSREtype>::TstMsgType, int vw) -> int {
      if (from == to) return 0;
      if ((from == 1 && to == 2) || (from == 2 && to == 1)) return 0;
      if ((from == 1 && to != 2) || (from == 2 && to != 1)) return 1;
      return (to == 2) || (to == 1);
    });
  for (int i = 0; i < 100; ++i) {
    if (vsreps[0].View() > 6 && vsreps[2].GetStatus() == Status::Normal
        && vsreps[3].View() > 6 && vsreps[3].GetStatus() == Status::Normal
        && vsreps[4].View() > 6 && vsreps[4].GetStatus() == Status::Normal)
      break;
    sleep_for(std::chrono::milliseconds(50));
  }

  ASSERT_EQ(8, vsreps[0].View());
  ASSERT_EQ(Status::Normal, vsreps[0].GetStatus());
  ASSERT_EQ(8, vsreps[3].View());
  ASSERT_EQ(Status::Normal, vsreps[3].GetStatus());
  ASSERT_EQ(8, vsreps[4].View());
  ASSERT_EQ(Status::Normal, vsreps[4].GetStatus());

  ASSERT_EQ(6, vsreps[1].View());
  ASSERT_EQ(Status::Normal, vsreps[1].GetStatus());
  ASSERT_EQ(6, vsreps[2].View());
  ASSERT_EQ(Status::Normal, vsreps[2].GetStatus());

  // Separated leader should not be able to commit an op without consensus followers
  ASSERT_EQ(2, vsreps[2].CommitID());
  ASSERT_EQ(2, vsreps[2].CommitID());
  vsreps[1].ConsumeMsg(MsgClientOp { 1568, "x=987" });
  ASSERT_EQ(3, vsreps[1].OpID());
  ASSERT_EQ(2, vsreps[1].CommitID());
  for (int i = 0; i < 21; ++i) {
    if (vsreps[1].OpID() == vsreps[1].CommitID())
      break;
    ASSERT_LT(i, 20);
    sleep_for(std::chrono::milliseconds(50));
  }
  ASSERT_EQ(2, vsreps[1].OpID());
  ASSERT_EQ(2, vsreps[1].CommitID());
  ASSERT_LT(vsreps[2].OpID(), 4); // could be 2 or 3
  ASSERT_EQ(2, vsreps[2].CommitID());
  // Meanwhile the island of leader should be able to persist ops
  vsreps[3].ConsumeMsg(MsgClientOp { 1571, "y=156" });
  ASSERT_EQ(3, vsreps[3].OpID());
  ASSERT_EQ(2, vsreps[3].CommitID());
  for (int i = 0; i < 21; ++i) {
    if (vsreps[3].OpID() == vsreps[3].CommitID())
      break;
    ASSERT_LT(i, 20);
    sleep_for(std::chrono::milliseconds(50));
  }
  ASSERT_EQ(3, vsreps[3].OpID());
  ASSERT_EQ(3, vsreps[3].CommitID());
  ASSERT_EQ(3, vsreps[0].OpID());
  ASSERT_EQ(3, vsreps[4].OpID());

  // --------------------------------------------------------------
  // Make replica:1-2 non-isolated again (join them to majority island)
  // --------------------------------------------------------------
  buggynw.SetDecideFun(
      [](int from, int to, FakeTMsgBuggyNetwork<VSREtype>::TstMsgType, int vw) { return 0; });
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
  for (int i = 0; i < 21; ++i) {
    if (vsreps[1].OpID() == 3 && vsreps[1].CommitID() == 3
        && vsreps[2].OpID() == 3 && vsreps[2].CommitID() == 3)
      break;
    ASSERT_LT(i, 20);
    sleep_for(std::chrono::milliseconds(50));
  }
  ASSERT_EQ(3, vsreps[1].CommitID());
  ASSERT_EQ(3, vsreps[2].CommitID());
}

}
}
