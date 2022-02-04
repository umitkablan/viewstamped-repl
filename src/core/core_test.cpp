#include "core_impl_test.cpp"

#include <gtest/gtest.h>

namespace vsrepl
{
namespace test
{

using ::testing::Eq;
using ::testing::A;
using ::testing::ElementsAre;
using ::testing::StrictMock;

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

}
}
