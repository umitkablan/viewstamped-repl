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
// using ::testing::TypedEq;
// using ::testing::_;
using ::testing::ElementsAre;
using ::testing::StrictMock;

using std::this_thread::sleep_for;
using VSRETestType = ViewstampedReplicationEngine<MockTMsgDispatcher, MockStateMachine>;

TEST(HasherTest, BasicHashAndMerge_Repeating)
{
  std::vector<std::pair<int, MsgClientOp>> vv {
    { 0, { 1, "x=3", 1 } },
    { 2, { 1, "x=4", 2 } },
    { 3, { 2, "y=0", 1 } },
    { 4, { 1, "x=-1", 3 } },
  };
  const auto hAll = mergeLogsHashes(vv.begin(), vv.end());
  const auto h0 = mergeLogsHashes(vv.begin(), vv.begin() + 2);
  const auto h2 = mergeLogsHashes(vv.begin() + 2, vv.begin() + 3, h0);
  const auto h3 = mergeLogsHashes(vv.begin() + 3, vv.begin() + 4, h2);

  ASSERT_EQ(hAll, h3);
  // below values change depending on (libc++) platform - fails during PR validation
  // ASSERT_EQ(std::size_t(10782676624795537932ull), h0);
  // ASSERT_EQ(std::size_t(11415403314268682002ull), h2);
  // ASSERT_EQ(std::size_t(15084498811304813772ull), h3);
}

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
  std::vector<std::pair<int, MsgPersistedCliOp>> cli_reqs;
  EXPECT_CALL(msgdispatcher, SendToClient(A<int>(), A<const MsgPersistedCliOp&>())).WillRepeatedly([&cli_reqs](int to, const MsgPersistedCliOp& pco) {
    ASSERT_EQ(0, pco.view);
    cli_reqs.push_back(std::make_pair(to, pco));
  });

  cr.HealthTimeoutTicked();
  ASSERT_THAT(res, ElementsAre(1, 2, 3, 4));
  res.clear();

  { // When ClientOp is received before Tick we optimize Prepare's
    for (int i = 0; i < 20; ++i) {
      cr.ConsumeMsg(MsgClientOp { 1231, "x=y", uint64_t(i + 18) });
      ASSERT_THAT(res, ElementsAre(1, 2, 3, 4));
      res.clear();
      cr.HealthTimeoutTicked();
      ASSERT_EQ(0, res.size()); // no prepares sent (for now)
      ASSERT_EQ(1, cli_reqs.size());
      ASSERT_EQ(uint64_t(i + 18), cli_reqs[0].second.cliopid);
      ASSERT_EQ(1231, cli_reqs[0].first);
      cli_reqs.clear();
    }

    cr.HealthTimeoutTicked(); // no optimization here since after ClientOp, ticked twice
    ASSERT_THAT(res, ElementsAre(1, 2, 3, 4));
    ASSERT_EQ(0, cli_reqs.size());
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
  std::vector<std::pair<int, MsgPersistedCliOp>> cli_reqs;
  EXPECT_CALL(msgdispatcher, SendToClient(A<int>(), A<const MsgPersistedCliOp&>())).WillRepeatedly([&cli_reqs](int to, const MsgPersistedCliOp& pco) {
    ASSERT_EQ(0, pco.view);
    cli_reqs.push_back(std::make_pair(to, pco));
  });

  {
    cr.ConsumeMsg(MsgClientOp { 1278, "xy=ert", 134 });
    ASSERT_EQ(0, cli_reqs.size());
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
    ASSERT_EQ(0, cli_reqs.size());
    ASSERT_EQ(4, recv.size());
    for (int i = 0; i < recv.size(); ++i) {
      ASSERT_EQ(i+1, recv[i].first);
      ASSERT_EQ(-1, recv[i].second.op);
    }
    recv.clear();
  }

  // Prepare timeouts and op is re-sent
  {
    cr.HealthTimeoutTicked();
    ASSERT_EQ(4, recv.size());
    ASSERT_EQ(0, cli_reqs.size());
    for (int i = 0; i < recv.size(); ++i) {
      ASSERT_EQ(i + 1, recv[i].first);
      ASSERT_EQ(-1, recv[i].second.op);
    }
    recv.clear();
    ASSERT_EQ(-1, cr.CommitID());
    ASSERT_EQ(0, cr.OpID());
  }

  cr.ConsumeReply(2, MsgPrepareResponse { "", -1 }); // replica:2 replies different op
  ASSERT_EQ(-1, cr.CommitID());
  ASSERT_EQ(0, cr.OpID());

  cr.ConsumeReply(2, MsgPrepareResponse { "", 0 }); // replica:2 replies correct op
  ASSERT_EQ(0, cr.CommitID());
  cr.ConsumeReply(1, MsgPrepareResponse { "", 0 }); // replica:1 replies again
  ASSERT_EQ(0, cr.CommitID());
  cr.ConsumeReply(2, MsgPrepareResponse { "", 0 }); // replica:2 replies again
  ASSERT_EQ(0, cr.CommitID());
  ASSERT_EQ(0, cr.OpID());

  cr.ConsumeReply(3, MsgPrepareResponse { "", 0 });
  {
    cr.ConsumeMsg(MsgClientOp { 1278, "zz=ttt" });
    ASSERT_EQ(1, cli_reqs.size());
    ASSERT_EQ(134, cli_reqs[0].second.cliopid);
    ASSERT_EQ(1278, cli_reqs[0].first);
    cli_reqs.clear();
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

  //
  // When ClientOp is received before Tick we optimize Prepare's
  {
    cr.ConsumeMsg(MsgClientOp { 1278, "dd=oprea", 789 });
    ASSERT_EQ(2, cr.OpID());
    ASSERT_EQ(1, cr.CommitID());
    ASSERT_EQ(4, recv.size());
    for (int i = 0; i < recv.size(); ++i) {
      ASSERT_EQ(i + 1, recv[i].first);
      ASSERT_EQ(2, recv[i].second.op);
    }
    recv.clear();
    cr.HealthTimeoutTicked();
    ASSERT_EQ(0, recv.size()); // nothing sent
    cr.ConsumeMsg(MsgClientOp { 1278, "ed=oea", 790 }); // not sent
    ASSERT_EQ(2, cr.OpID());
    ASSERT_EQ(1, cr.CommitID());
    ASSERT_EQ(0, recv.size());
  }
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
  std::vector<std::pair<int, MsgPersistedCliOp>> cli_reqs;
  EXPECT_CALL(msgdispatcher, SendToClient(A<int>(), A<const MsgPersistedCliOp&>())).WillRepeatedly([&cli_reqs, view](int to, const MsgPersistedCliOp& pco) {
    ASSERT_EQ(view, pco.view);
    cli_reqs.push_back(std::make_pair(to, pco));
  });

  {
    cr1.ConsumeMsg(leader, MsgPrepare { view, -1, -1, 0, MsgClientOp{} });
    ASSERT_EQ(0, missing_log_reqs.size());
    ASSERT_EQ(0, cli_reqs.size());
  }
  ASSERT_EQ(-1, cr1.OpID());
  {
    cr1.ConsumeMsg(leader, MsgPrepare { view, 0, -1, 0, MsgClientOp{1336, "xz=efr", 17} });
    ASSERT_EQ(0, missing_log_reqs.size());
    ASSERT_EQ(0, cli_reqs.size());
  }
  ASSERT_EQ(0, cr1.OpID());
  {
    const auto hh = cr1.GetHash();
    cr1.ConsumeMsg(leader, MsgPrepare { view, 0, 0, hh, MsgClientOp { 1237, "" } });
    ASSERT_EQ(0, missing_log_reqs.size());
    ASSERT_EQ(1, cli_reqs.size());
    ASSERT_EQ(17, cli_reqs[0].second.cliopid);
    ASSERT_EQ(1336, cli_reqs[0].first);
    cli_reqs.clear();
  }
  ASSERT_EQ(0, cr1.CommitID());
  ASSERT_EQ(0, cr1.OpID());

  {
    const auto hh = cr1.GetHash();
    cr1.ConsumeMsg(leader, MsgPrepare { view, 4, 0, hh, MsgClientOp{1297, "xzz=efrs", 19} });
    ASSERT_EQ(0, cr1.CommitID());
    ASSERT_EQ(4, cr1.OpID());
    ASSERT_EQ(0, missing_log_reqs.size());
    const auto hh2 = cr1.GetHash();
    cr1.ConsumeMsg(leader, MsgPrepare { view, 5, 4, hh2, MsgClientOp{1277, "azx=342", 21} });
    ASSERT_EQ(4, cr1.CommitID());
    ASSERT_EQ(5, cr1.OpID());
    ASSERT_EQ(0, missing_log_reqs.size());
    ASSERT_EQ(1, cli_reqs.size());
    ASSERT_EQ(19, cli_reqs[0].second.cliopid);
    ASSERT_EQ(1297, cli_reqs[0].first);
    cli_reqs.clear();
  }
  ASSERT_EQ(4, cr1.CommitID());
  ASSERT_EQ(5, cr1.OpID());

  {
    cr1.ConsumeMsg(leader, MsgPrepare { view, 7, 6, 34455, MsgClientOp{1237, "xzz=efrs"} });
    ASSERT_EQ(1, missing_log_reqs.size());
    ASSERT_EQ(leader, missing_log_reqs[0].first);
    ASSERT_EQ(4, missing_log_reqs[0].second.my_last_commit);
    cr1.ConsumeReply(leader, MsgMissingLogsResponse { view, "",
        { 7, MsgClientOp{ 1237, "ss=45", 113 } },
        {
          { 6, MsgClientOp{ 1237, "ee=dd", 112 } },
        }
      });
    ASSERT_EQ(1, cli_reqs.size());
    ASSERT_EQ(112, cli_reqs[0].second.cliopid);
    ASSERT_EQ(1237, cli_reqs[0].first);
    cli_reqs.clear();
  }
  cr1.HealthTimeoutTicked();
  cr1.HealthTimeoutTicked();
  ASSERT_EQ(6, cr1.CommitID());
  ASSERT_EQ(7, cr1.OpID());
}

TEST(CoreTest, PrevLeaderDiscardsCommitIfLeaderDontKnow0)
{
  StrictMock<MockTMsgDispatcher> msgdispatcher;
  MockStateMachine sm;
  VSRETestType cr0(3, 0, msgdispatcher, sm); // 0 is leader by default

  const int view_cur = 6, view_next = 7, leader_cur = 0, leader_next = 1;

  std::vector<std::pair<int, MsgPersistedCliOp>> cli_reqs;
  EXPECT_CALL(msgdispatcher, SendToClient(A<int>(), A<const MsgPersistedCliOp&>())).WillRepeatedly([&cli_reqs](int to, const MsgPersistedCliOp& pco) {
    cli_reqs.push_back(std::make_pair(to, pco));
  });

  cr0.ConsumeMsg(leader_cur, MsgPrepare { view_cur, 0, -1, 0, MsgClientOp { 1445, "to0=x", 1 } });
  cr0.ConsumeReply(leader_cur, MsgPrepareResponse { "", 0 }); // leader:0 will persist
  ASSERT_EQ(0, cr0.CommitID());
  ASSERT_EQ(1, cli_reqs.size());
  ASSERT_EQ(1, cli_reqs[0].second.cliopid);
  ASSERT_EQ(1445, cli_reqs[0].first);
  cli_reqs.clear();
  // however, as leader_next doesn't know about this commit, it should be reverted
  cr0.ConsumeMsg(leader_next, MsgPrepare { view_next, -1, -1, 0, MsgClientOp {} });
  ASSERT_EQ(-1, cr0.CommitID());
  ASSERT_EQ(0, cr0.GetHash());
  ASSERT_EQ(0, cli_reqs.size());
}

TEST(CoreTest, PrevLeaderDiscardsCommitIfLeaderDontKnow1)
{
  StrictMock<MockTMsgDispatcher> msgdispatcher;
  MockStateMachine sm;
  VSRETestType cr0(3, 0, msgdispatcher, sm); // 0 is leader by default

  const int view_cur = 6, view_next = 7, leader_cur = 0, leader_next = 1;

  std::vector<std::pair<int, MsgPersistedCliOp>> cli_reqs;
  EXPECT_CALL(msgdispatcher, SendToClient(A<int>(), A<const MsgPersistedCliOp&>())).WillRepeatedly([&cli_reqs](int to, const MsgPersistedCliOp& pco) {
    cli_reqs.push_back(std::make_pair(to, pco));
  });

  cr0.ConsumeMsg(leader_cur, MsgPrepare { view_cur, 0, -1, 0, MsgClientOp { 1445, "to0=x", 1 } });
  cr0.ConsumeReply(leader_cur, MsgPrepareResponse { "", 0 }); // leader:0 will persist
  ASSERT_EQ(0, cr0.CommitID());
  ASSERT_EQ(1, cli_reqs.size());
  ASSERT_EQ(1, cli_reqs[0].second.cliopid);
  ASSERT_EQ(1445, cli_reqs[0].first);
  cli_reqs.clear();
  // however, as leader_next doesn't know about this commit, it should be reverted
  cr0.ConsumeMsg(leader_next, MsgPrepare { view_next, 0, -1, 0, MsgClientOp { 123, "to1=y", 1 } });
  ASSERT_EQ(-1, cr0.CommitID());
  ASSERT_EQ(0, cr0.GetHash());
  ASSERT_EQ(0, cli_reqs.size());
}

TEST(CoreTest, PrevLeaderDiscardsCommitIfLeaderDontKnow2)
{
  StrictMock<MockTMsgDispatcher> msgdispatcher;
  MockStateMachine sm;
  VSRETestType cr0(3, 0, msgdispatcher, sm); // 0 is leader by default

  const int view_cur = 6, view_next = 7, leader_cur = 0, leader_next = 1;
  std::vector<std::pair<int, MsgGetMissingLogs>> missing_log_reqs;
  EXPECT_CALL(msgdispatcher, SendMsg(A<int>(), A<const MsgGetMissingLogs&>())).WillRepeatedly([&missing_log_reqs, view_next](int to, const MsgGetMissingLogs& gml) {
    ASSERT_EQ(view_next, gml.view);
    missing_log_reqs.push_back(std::make_pair(to, gml));
  });
  std::vector<std::pair<int, MsgPersistedCliOp>> cli_reqs;
  EXPECT_CALL(msgdispatcher, SendToClient(A<int>(), A<const MsgPersistedCliOp&>())).WillRepeatedly([&cli_reqs](int to, const MsgPersistedCliOp& pco) {
    cli_reqs.push_back(std::make_pair(to, pco));
  });

  cr0.ConsumeMsg(leader_cur, MsgPrepare { view_cur, 0, -1, 0, MsgClientOp { 1445, "to0=x", 1 } });
  cr0.ConsumeReply(leader_cur, MsgPrepareResponse { "", 0 }); // leader:0 will persist
  ASSERT_EQ(0, cr0.CommitID());
  ASSERT_EQ(0, missing_log_reqs.size());
  ASSERT_EQ(1, cli_reqs.size());
  ASSERT_EQ(1, cli_reqs[0].second.cliopid);
  ASSERT_EQ(1445, cli_reqs[0].first);
  cli_reqs.clear();
  // however, as leader_next doesn't know about this commit, it should be reverted
  cr0.ConsumeMsg(leader_next, MsgPrepare { view_next, 1, 0, 6747, MsgClientOp { 123, "to1=y", 2 } });
  ASSERT_EQ(-1, cr0.CommitID());
  ASSERT_EQ(-1, cr0.OpID());
  ASSERT_EQ(0, cr0.GetHash());

  ASSERT_EQ(1, missing_log_reqs.size());
  ASSERT_EQ(leader_next, missing_log_reqs[0].first);
  ASSERT_EQ(-1, missing_log_reqs[0].second.my_last_commit);
  ASSERT_EQ(0, cli_reqs.size());

  const auto& logs = cr0.GetCommittedLogs();
  ASSERT_EQ(0, logs.size());
}

TEST(CoreWithBuggyNetwork, ViewChange_BuggyNetworkNoShuffle_Scenarios)
{
  using VSREtype = ViewstampedReplicationEngine<ParentMsgDispatcher, MockStateMachine>;
  using vsrCliTyp = VSReplCli<ParentMsgDispatcher>;
  using testMessageTyp = FakeTMsgBuggyNetwork<VSREtype,vsrCliTyp>::TstMsgType;
  const int clientMinIdx = 50;

  FakeTMsgBuggyNetwork<VSREtype, vsrCliTyp> buggynw(clientMinIdx,
    [](int from, int to, testMessageTyp, int vw) {
      return 0;
    }, true);
  std::vector<ParentMsgDispatcher> nwdispatchers {
    { 0, &buggynw }, { 1, &buggynw }, { 2, &buggynw }, { 3, &buggynw }, { 4, &buggynw },
    { clientMinIdx, &buggynw },
  };
  std::vector<MockStateMachine> statemachines(5);
  std::vector<VSREtype> vsreps;
  vsreps.reserve(5); // we need explicit push_back due to copy constructor absence
  vsreps.push_back({ 5, 0, nwdispatchers[0], statemachines[0] });
  vsreps.push_back({ 5, 1, nwdispatchers[1], statemachines[1] });
  vsreps.push_back({ 5, 2, nwdispatchers[2], statemachines[2] });
  vsreps.push_back({ 5, 3, nwdispatchers[3], statemachines[3] });
  vsreps.push_back({ 5, 4, nwdispatchers[4], statemachines[4] });

  auto vsc0 = std::make_unique<vsrCliTyp>(clientMinIdx, nwdispatchers[5], int(vsreps.size()));
  buggynw.SetEnginesStart(std::vector<VSREtype*> {
    &vsreps[0], &vsreps[1], &vsreps[2], &vsreps[3], &vsreps[4] },
    std::vector<vsrCliTyp*> { vsc0.get() });
  std::shared_ptr<void> buggynwDel(nullptr,
    [&buggynw](void*) { buggynw.CleanEnginesStop(); });


  buggynw.SendMsg(-1, 0, MsgClientOp{ clientMinIdx, "x=12", 86 });
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
  cout << "*** make replica:0 isolated (receive & send) -> Changes to view:1 automatically" << endl;
  buggynw.SetDecideFun(
    [](int from, int to, testMessageTyp, int vw) -> int {
      if (from == to) return 0;
      return from==0 || to==0;
    });
  // We can only (and safely) communicate with replica:0 directly
  vsreps[0].ConsumeMsg(MsgClientOp { 1212, "x=to0_isolated0_v0to1-1314", 287 });
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
  ASSERT_EQ(1, vsreps[0].OpID());

  int cnt = 0;
  for (const auto& rep : vsreps) {
    if (rep.View() == 1 && rep.GetStatus() == Status::Normal)
      ++cnt;
  }
  ASSERT_THAT(cnt, ::testing::Gt(3));

  vsreps[0].ConsumeMsg(MsgClientOp{ clientMinIdx, "x=to0_isolated0_v1-1314", 88 });
  // re-join replica:0
  cout << "*** re-join replica:0" << endl;
  buggynw.SetDecideFun(
      [](int from, int to, testMessageTyp, int vw) { return 0; });
  for (int i = 0; i < 40; ++i) {
    if (vsreps[0].View() > 0 && vsreps[0].GetStatus() == Status::Normal) break;
    sleep_for(std::chrono::milliseconds(50));
  }
  ASSERT_EQ(1, vsreps[0].View());
  ASSERT_EQ(Status::Normal, vsreps[0].GetStatus());

  // --------------------------------------------------------------
  // make replica:1 isolated (receive-only, block outgoing messages)
  // --------------------------------------------------------------
  cout << "*** make replica:1 isolated (receive-only, block outgoing messages)" << endl;

  buggynw.SetDecideFun(
    [](int from, int to, testMessageTyp, int vw) -> int {
      if (from == to) return 0;
      return from==1;
    });
  vsreps[1].ConsumeMsg(MsgClientOp{ clientMinIdx, "x=to1_isolated1_v1to2-6655", 89 });
  for (int i = 0; i < 100; ++i) {
    if (vsreps[0].View() > 1 && vsreps[0].GetStatus() == Status::Normal
        && vsreps[2].View() > 1 && vsreps[2].GetStatus() == Status::Normal
        && vsreps[3].View() > 1 && vsreps[3].GetStatus() == Status::Normal
        && vsreps[4].View() > 1 && vsreps[4].GetStatus() == Status::Normal)
      break;
    sleep_for(std::chrono::milliseconds(50));
  }
  vsreps[2].ConsumeMsg(MsgClientOp{ clientMinIdx, "x=to2_isolated1_v2-6655", 90 });
  ASSERT_EQ(1, vsreps[2].OpID());
  ASSERT_EQ(0, vsreps[2].CommitID());
  for (int i = 0; i < 21; ++i) {
    if (vsreps[2].OpID() == vsreps[2].CommitID())
      break;
    ASSERT_LT(i, 20);
    sleep_for(std::chrono::milliseconds(50));
  }
  {
    const auto& logs = vsreps[2].GetCommittedLogs();
    ASSERT_EQ(logs.size(), 2);
    ASSERT_EQ(std::make_pair(0, MsgClientOp { clientMinIdx, "x=12", 86 }), logs[0]);
    ASSERT_EQ(std::make_pair(1, MsgClientOp { clientMinIdx, "x=to2_isolated1_v2-6655", 90 }), logs.back());
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

  { // assert that replica:1 could also commit
    for (int i = 0; i < 21; ++i) {
      if (vsreps[1].OpID() == vsreps[1].CommitID())
        break;
      ASSERT_LT(i, 20);
      sleep_for(std::chrono::milliseconds(50));
    }
    ASSERT_EQ(vsreps[1].OpID(), 1);
    ASSERT_EQ(vsreps[1].CommitID(), 1);
    const auto& logs = vsreps[1].GetCommittedLogs();
    ASSERT_EQ(logs.size(), 2);
    ASSERT_EQ(std::make_pair(0, MsgClientOp{ clientMinIdx, "x=12", 86 }), logs[0]);
    ASSERT_EQ(std::make_pair(1, MsgClientOp{ clientMinIdx, "x=to2_isolated1_v2-6655", 90 }), logs.back());
  }

  // make replica:1 messages pass to destinations, again
  cout << "*** make replica:1 messages pass to destinations, again" << endl;
  buggynw.SetDecideFun(
      [](int from, int to, testMessageTyp, int vw) { return 0; });
  // since replica:1 was isolated receive-only, it should have correct view ASAP, no poll needed
  ASSERT_EQ(2, vsreps[1].View());
  ASSERT_EQ(Status::Normal, vsreps[1].GetStatus());
  {
    const auto& logs = vsreps[1].GetCommittedLogs();
    ASSERT_EQ(logs.size(), 2);
    ASSERT_EQ(std::make_pair(0, MsgClientOp{ clientMinIdx, "x=12", 86 }), logs[0]);
    ASSERT_EQ(std::make_pair(1, MsgClientOp{ clientMinIdx, "x=to2_isolated1_v2-6655", 90 }), logs.back());
  }

  // --------------------------------------------------------------
  // make replica:2-3 isolated (receive & send) -> Changes to view:4 automatically
  // --------------------------------------------------------------
  cout << "*** make replica:2-3 isolated (receive & send) -> Changes to view:4 automatically" << endl;
  buggynw.SetDecideFun(
    [](int from, int to, testMessageTyp, int vw) -> int {
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
    ASSERT_EQ(std::make_pair(0, MsgClientOp{ clientMinIdx, "x=12", 86 }), logs[0]);
    ASSERT_EQ(std::make_pair(1, MsgClientOp{ clientMinIdx, "x=to2_isolated1_v2-6655", 90 }), logs.back());
  }
  for (int i = 0; i < 21; ++i) {
    if (vsreps[1].CommitID() > 0)
      break;
    ASSERT_LT(i, 20);
    sleep_for(std::chrono::milliseconds(50));
  }
  {
    const auto& logs = vsreps[1].GetCommittedLogs();
    ASSERT_EQ(logs.size(), 2);
    ASSERT_EQ(std::make_pair(0, MsgClientOp{ clientMinIdx, "x=12", 86 }), logs[0]);
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
      [](int from, int to, testMessageTyp, int vw) { return 0; });
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
      [](int from, int to, testMessageTyp, int vw) -> int {
        if (from == to) return 0;
        return (from == 4 && to != 0) || (from == 0 && to != 4); //from == 4 || from == 0;
      });
  vsreps[4].ConsumeMsg(MsgClientOp{ clientMinIdx, "xt=55", 90 });
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

  vsreps[4].ConsumeMsg(MsgClientOp{ clientMinIdx, "xt=to4_isolated40_v4to6-002", 91 });
  vsreps[1].ConsumeMsg(MsgClientOp{ clientMinIdx, "xu=75", 92 });
  for (int i = 0; i < 21; ++i) {
    if (vsreps[1].CommitID() > 1) //vsreps[1].OpID() == vsreps[1].CommitID()
      break;
    ASSERT_LT(i, 20);
    sleep_for(std::chrono::milliseconds(50));
  }
  {
    const auto& logs = vsreps[1].GetCommittedLogs();
    ASSERT_GT(logs.size(), 2);
    ASSERT_EQ(std::make_pair(0, MsgClientOp{ clientMinIdx, "x=12", 86 }), logs[0]);
    ASSERT_EQ(std::make_pair(1, MsgClientOp{ clientMinIdx, "x=to2_isolated1_v2-6655", 90}), logs[1]);
    ASSERT_EQ(std::make_pair(2, MsgClientOp{ clientMinIdx, "xu=75", 92 }), logs.back());
  }
  ASSERT_EQ(2, vsreps[1].CommitID());

  // re-join replica:4-0
  cout << "*** re-join replica:4-0" << endl;
  buggynw.SetDecideFun(
      [](int from, int to, testMessageTyp, int vw) { return 0; });
  vsreps[1].ConsumeMsg(MsgClientOp{ clientMinIdx, "xu=to1_joining40_beforeisolating12_v6-4", 93 });
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
  for (int i = 0; i < 21; ++i) {
    // Checking OpID instead of CommitID would have sporadic behavior where the log could be
    //   discarded when it is not *yet _committed_ to the majority* - better be consistent
    if (vsreps[0].CommitID() == 3 && vsreps[4].CommitID() == 3)
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
  cout << "*** make replica:1-2 an island of network separate from the rest" << endl;

  buggynw.SetDecideFun(
    [](int from, int to, testMessageTyp, int vw) -> int {
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
  ASSERT_EQ(3, vsreps[2].CommitID());
  ASSERT_EQ(3, vsreps[2].CommitID());
  vsreps[1].ConsumeMsg(MsgClientOp{ clientMinIdx, "x=to1_separated12_v6to8", 94 });
  ASSERT_EQ(4, vsreps[1].OpID());
  ASSERT_EQ(3, vsreps[1].CommitID());
  for (int i = 0; i < 21; ++i) {
    if (vsreps[2].OpID() == 4)
      break;
    ASSERT_LT(i, 20);
    sleep_for(std::chrono::milliseconds(50));
  }
  ASSERT_EQ(3, vsreps[2].CommitID());
  // Meanwhile the island of leader should be able to persist ops
  vsreps[1].ConsumeMsg(MsgClientOp{ clientMinIdx, "x=to1_isolated12_v6to8-1232", 95 });
  vsreps[3].ConsumeMsg(MsgClientOp{ clientMinIdx, "y=to3_isolated12_v8-1563", 96 });
  ASSERT_EQ(4, vsreps[3].OpID());
  ASSERT_EQ(3, vsreps[3].CommitID());
  for (int i = 0; i < 21; ++i) {
    if (vsreps[3].OpID() == vsreps[3].CommitID())
      break;
    ASSERT_LT(i, 20);
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
      [](int from, int to, testMessageTyp, int vw) { return 0; });
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
    if (vsreps[0].OpID() == 4 && vsreps[0].CommitID() == 4
        && vsreps[1].OpID() == 4 && vsreps[1].CommitID() == 4
        && vsreps[2].OpID() == 4 && vsreps[2].CommitID() == 4)
      break;
    ASSERT_LT(i, 20);
    sleep_for(std::chrono::milliseconds(50));
  }

  {
    auto&& logs = vsreps[1].GetCommittedLogs();
    ASSERT_EQ(5, logs.size());
    ASSERT_EQ(std::make_pair(0, MsgClientOp{ clientMinIdx, "x=12", 86 }), logs[0]);
    ASSERT_EQ(std::make_pair(1, MsgClientOp{ clientMinIdx, "x=to2_isolated1_v2-6655", 90 }), logs[1]);
    ASSERT_EQ(std::make_pair(2, MsgClientOp{ clientMinIdx, "xu=75", 92 }), logs[2]);
    ASSERT_EQ(std::make_pair(3, MsgClientOp{ clientMinIdx, "xu=to1_joining40_beforeisolating12_v6-4", 93 }), logs[3]);
    ASSERT_EQ(std::make_pair(4, MsgClientOp{ clientMinIdx, "y=to3_isolated12_v8-1563", 96 }), logs[4]);
  }
  {
    auto&& logs = vsreps[0].GetCommittedLogs();
    ASSERT_EQ(5, logs.size());
    ASSERT_EQ(std::make_pair(0, MsgClientOp{ clientMinIdx, "x=12", 86 }), logs[0]);
    ASSERT_EQ(std::make_pair(1, MsgClientOp{ clientMinIdx, "x=to2_isolated1_v2-6655", 90 }), logs[1]);
    ASSERT_EQ(std::make_pair(2, MsgClientOp{ clientMinIdx, "xu=75", 92 }), logs[2]);
    ASSERT_EQ(std::make_pair(3, MsgClientOp{ clientMinIdx, "xu=to1_joining40_beforeisolating12_v6-4", 93 }), logs[3]);
    ASSERT_EQ(std::make_pair(4, MsgClientOp{ clientMinIdx, "y=to3_isolated12_v8-1563", 96 }), logs[4]);
  }
}

}
}
