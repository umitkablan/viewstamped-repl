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

using vsrCliTyp = VSReplCli<StrictMock<MockTMsgDispatcher>>;

TEST(CliTest, ClientBasicStartDelete)
{
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

}
}
