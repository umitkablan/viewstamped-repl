#include "coresm.hpp"

#include <gtest/gtest.h>

namespace vsrepl {
namespace test {

TEST(CoreStateMachineTest, BasicDoViewChange)
{
  VSREngineCore cr(3, 2);
  for (int i=0; i<2; ++i)
  {
    const auto res = cr.HealthTimeoutTicked();
    ASSERT_EQ(0, res.index());
  }
  {
    const auto res = cr.HealthTimeoutTicked();
    ASSERT_EQ(2, res.index());
    auto&& svcs = std::get<VSREngineCore::SVCMsgsType>(res);
    ASSERT_EQ(3, svcs.size());
    for (int i = 0; i < 3; ++i) {
      ASSERT_EQ(i, svcs[i].first);
      ASSERT_EQ(1, svcs[i].second.view);
    }
  }
  {
    auto res = cr.SVCReceived(1, MsgStartViewChange { 1 });
    ASSERT_EQ(0, res.index());
    res = cr.SVCReceived(2, MsgStartViewChange { 1 });
    ASSERT_EQ(1, res.index());
    ASSERT_EQ(1, get<1>(res).first);
    ASSERT_EQ(1, get<1>(res).second.view);
  }
}

TEST(CoreStateMachineTest, FilterDuplicateSVCs)
{
  VSREngineCore cr(5, 4);
  for (int i = 0; i < 2; ++i) {
    const auto res = cr.HealthTimeoutTicked();
    ASSERT_EQ(0, res.index());
  }
  {
    const auto res = cr.HealthTimeoutTicked();
    ASSERT_EQ(2, res.index());
    auto&& svcs = std::get<VSREngineCore::SVCMsgsType>(res);
    ASSERT_EQ(5, svcs.size());
    for (int i = 0; i < 5; ++i)
      ASSERT_EQ(i, svcs[i].first);
  }
  {
    auto res = cr.SVCReceived(1, MsgStartViewChange { 1 });
    ASSERT_EQ(0, res.index());
    // Filter these and expect replica!=1 to send SVC to proceed to DVC
    res = cr.SVCReceived(1, MsgStartViewChange { 1 });
    ASSERT_EQ(0, res.index());
    res = cr.SVCReceived(1, MsgStartViewChange { 1 });
    ASSERT_EQ(0, res.index());
  }
  {
    auto res = cr.SVCReceived(4, MsgStartViewChange { 1 });
    ASSERT_EQ(0, res.index());
    res = cr.SVCReceived(2, MsgStartViewChange { 1 });
    ASSERT_EQ(1, res.index());
    ASSERT_EQ(1, std::get<1>(res).first);
    ASSERT_EQ(1, std::get<1>(res).second.view);
  }
}

TEST(CoreStateMachineTest, FilterDuplicateSVCsWhileViewInc)
{
  VSREngineCore cr(5, 4);
  for (int i = 0; i < 2; ++i) {
    const auto res = cr.HealthTimeoutTicked();
    ASSERT_EQ(0, res.index());
  }
  {
    const auto res = cr.HealthTimeoutTicked();
    ASSERT_EQ(2, res.index());
    auto&& svcs = std::get<2>(res);
    ASSERT_EQ(5, svcs.size());
    for (int i = 0; i < 5; ++i)
      ASSERT_EQ(i, svcs[i].first);
  }
  {
    auto res = cr.SVCReceived(1, MsgStartViewChange { 1 });
    ASSERT_EQ(0, res.index());
    // Filter these and expect replica!=1 to send SVC to proceed to DVC
    res = cr.SVCReceived(1, MsgStartViewChange { 1 });
    ASSERT_EQ(0, res.index());
    res = cr.SVCReceived(1, MsgStartViewChange { 1 });
    ASSERT_EQ(0, res.index());
  }
  {
    auto res = cr.SVCReceived(3, MsgStartViewChange { 2 });
    ASSERT_EQ(0, res.index());
    res = cr.SVCReceived(3, MsgStartViewChange { 2 });
    ASSERT_EQ(0, res.index());
    res = cr.SVCReceived(2, MsgStartViewChange { 2 });
    ASSERT_EQ(0, res.index());
    res = cr.SVCReceived(4, MsgStartViewChange { 2 });
    ASSERT_EQ(1, res.index());
    ASSERT_EQ(2, std::get<1>(res).first);
    ASSERT_EQ(2, std::get<1>(res).second.view);
  }
}

TEST(CoreStateMachineTest, DVCWhenOthersRecognizeLeaderDead)
{
  VSREngineCore cr(5, 4);
  // I got one unmet tick, not enough to emit SVC
  {
    const auto res = cr.HealthTimeoutTicked();
    ASSERT_EQ(0, res.index());
  }
  // However someone else noticed leader inactivity in the mean time; join the party
  {
    const auto res = cr.SVCReceived(2, MsgStartViewChange { 1 });
    ASSERT_EQ(2, res.index());
    auto&& svcs = std::get<2>(res);
    ASSERT_EQ(5, svcs.size());
    for (int i = 0; i < 5; ++i) {
      ASSERT_EQ(i, svcs[i].first);
      ASSERT_EQ(1, svcs[i].second.view);
    }
  }
  // And then, received another unmet tick
  {
    auto res = cr.HealthTimeoutTicked();
    ASSERT_EQ(0, res.index());
    res = cr.HealthTimeoutTicked();
    ASSERT_EQ(2, res.index());
    auto&& svcs = std::get<2>(res);
    ASSERT_EQ(5, svcs.size());
    for (int i = 0; i < 5; ++i) {
      ASSERT_EQ(i, svcs[i].first);
      ASSERT_EQ(1, svcs[i].second.view);
    }
  }
}

}
}
