#include "core_impl_test.cpp"

#include <array>
#include <future>
#include <gtest/gtest.h>
#include <mutex>

#include <iostream>
using std::cout;
using std::endl;

namespace vsrepl {
namespace test {

using ::testing::Eq;
using ::testing::A;
using ::testing::ElementsAre;

using std::this_thread::sleep_for;

template <typename TTimestampedReplEngine>
class FakeTMsgBuggyNetwork : public INetDispatcher {
public:
  enum class TstMsgType : char {
    ClientOp,
    StartViewCh,
    DoViewCh,
    StartView,
    Prepare,
    StartViewResponse,
    PrepareResponse,
  };

  FakeTMsgBuggyNetwork(std::function<int(int, int, TstMsgType, int)> decFun, bool shuffle = false)
    : random_chose_border_((RAND_MAX / 3) * 2)
    , is_shuffle_(shuffle)
    , decide_(decFun)
    , break_thread_(false)
  {
  }

  void SetEnginesStart(std::vector<TTimestampedReplEngine*> engines)
  {
    if (th_.joinable())
      throw std::invalid_argument("network thread is running");

    engines_mtxs_ = std::vector<std::mutex>(engines.size());
    engines_ = std::move(engines);

    break_thread_ = false;
    th_ = std::thread([this]() { threadTask(); });
    for (auto& e : engines_)
      e->Start();
  }

  void CleanEnginesStop()
  {
    for (auto& e : engines_)
      e->Stop();

    break_thread_ = true;
    if (th_.joinable())
      th_.join();
    finishEnqueuedTasks();

    engines_.clear();
    engines_mtxs_.clear();
  }

  void SetDecideFun(std::function<int(int, int, TstMsgType, int)> decFun)
  {
    std::lock_guard<std::mutex> lck(decide_mtx_);
    decide_ = decFun;
  }

  void SendMsg(int from, int to, const MsgClientOp& cliop) override
  {
    enqueueTask(pts_, [from, to, cliop, this]() {
      auto ret = callDecideSync(from, to, TstMsgType::ClientOp, -1);
      if (!ret) {
        std::lock_guard<std::mutex> lck(engines_mtxs_[to]);
        ret = engines_[to]->ConsumeMsg(cliop);
      }
      return ret;
    });
  }

  void SendMsg(int from, int to, const MsgStartViewChange& svc) override
  {
    enqueueTask(pts_, [from, to, svc, this]() {
      auto ret = callDecideSync(from, to, TstMsgType::StartViewCh, svc.view);
      if (!ret) {
        std::lock_guard<std::mutex> lck(engines_mtxs_[to]);
        ret = engines_[to]->ConsumeMsg(from, svc);
      }
      return ret;
    });
  }

  void SendMsg(int from, int to, const MsgDoViewChange& dvc) override
  {
    enqueueTask(pts_, [from, to, dvc, this]() {
      auto ret = callDecideSync(from, to, TstMsgType::DoViewCh, dvc.view);
      if (!ret) {
        std::lock_guard<std::mutex> lck(engines_mtxs_[to]);
        ret = engines_[to]->ConsumeMsg(from, dvc);
      }
      return ret;
    });
  }

  void SendMsg(int from, int to, const MsgStartView& sv) override
  {
    enqueueTask(pts_, [from, to, sv, this]() {
      auto ret = callDecideSync(from, to, TstMsgType::StartView, sv.view);
      MsgStartViewResponse svr{ sv.view, "failxd-13 network" };
      if (!ret) {
        std::lock_guard<std::mutex> lck(engines_mtxs_[to]);
        svr = engines_[to]->ConsumeMsg(from, sv);
      }
      SendMsg(to, from, svr);
      return ret;
    });
  }

  void SendMsg(int from, int to, const MsgPrepare& pr) override
  {
    enqueueTask(pts_, [from, to, pr, this]() {
      auto ret = callDecideSync(from, to, TstMsgType::Prepare, pr.view);
      MsgPrepareResponse presp { pr.view, "err asdeee" };
      if (!ret) {
        std::lock_guard<std::mutex> lck(engines_mtxs_[to]);
        presp = engines_[to]->ConsumeMsg(from, pr);
      }
      SendMsg(to, from, presp);
      return ret;
    });
  }

  void SendMsg(int from, int to, const MsgStartViewResponse& svr) override
  {
    enqueueTask(pts_, [from, to, svr, this]() {
      auto ret = callDecideSync(from, to, TstMsgType::StartViewResponse, svr.view);
      if (!ret) {
        std::lock_guard<std::mutex> lck(engines_mtxs_[to]);
        ret = engines_[to]->ConsumeReply(from, svr);
      }
      return ret;
    });
  }

  void SendMsg(int from, int to, const MsgPrepareResponse& presp) override
  {
    enqueueTask(pts_, [from, to, presp, this]() {
      auto ret = callDecideSync(from, to, TstMsgType::PrepareResponse, presp.view);
      if (!ret) {
        std::lock_guard<std::mutex> lck(engines_mtxs_[to]);
        ret = engines_[to]->ConsumeReply(from, presp);
      }
      return ret;
    });
  }

private:
  int random_chose_border_;
  bool is_shuffle_;
  std::vector<TTimestampedReplEngine*> engines_;
  std::vector<std::mutex> engines_mtxs_;
  std::mutex decide_mtx_;
  std::function<int(int, int, TstMsgType, int)> decide_;

  bool break_thread_;
  std::thread th_;

  mutable std::mutex packs_mtx_;

  std::vector<std::packaged_task<int()>> pts_;

  int callDecideSync(int from, int to, TstMsgType ty, int view)
  {
    std::lock_guard<std::mutex> lck(decide_mtx_);
    return decide_(from, to, ty, view);
  }

  template <typename PtCont, typename Fun>
  auto enqueueTask(PtCont& c, Fun&& f) -> std::future<typename std::result_of<Fun()>::type>
  {
    typename PtCont::value_type pt(std::move(f));
    auto fut = pt.get_future();
    {
      std::lock_guard<std::mutex> lck(packs_mtx_);

      if (c.empty())
        c.push_back(std::move(pt));
      else {
        auto i = is_shuffle_ ? (std::rand() % (c.size() + 1)) : 0;
        c.insert(c.begin() + i, std::move(pt));
      }
    }
    return fut;
  }

  template <typename Cont>
  auto popLastOf(Cont& c) -> std::pair<bool, typename Cont::value_type>
  {
    std::pair<bool, typename Cont::value_type> ret;
    ret.first = false;
    std::lock_guard<std::mutex> lck(packs_mtx_);

    if (c.empty())
      return ret;
    ret.first = true;
    ret.second = std::move(c.back());
    c.pop_back();
    return ret;
  }

  void threadTask()
  {
    while (!break_thread_) {
      sleep_for(std::chrono::milliseconds(5));
      auto [found, pt] = popLastOf(pts_);
      if (found) {
        std::thread([p = std::move(pt)]() mutable { p(); }).detach();
      }
    }
  }

  int finishEnqueuedTasks()
  {
    while(true) {
      auto [found, pt] = popLastOf(pts_);
      if (!found) break;
      std::thread([p = std::move(pt)]() mutable { p(); }).detach();
    }
    return pts_.size();
  }
};

TEST(CoreTest, ViewChange_BuggyNetworkNoShuffle_IsolateLeader0)
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
    if (vsreps[0].CommitID() == 0)
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
    [](int from, int to, FakeTMsgBuggyNetwork<VSREtype>::TstMsgType, int vw) {
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
  // CommitID is not 0+1 since it's isolated and cannot receive PrepareOK
  ASSERT_EQ(0, vsreps[0].CommitID());
  ASSERT_EQ(1, vsreps[0].OpID());

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
      [](int from, int to, FakeTMsgBuggyNetwork<VSREtype>::TstMsgType, int vw) { return from==1; });
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
      [](int from, int to, FakeTMsgBuggyNetwork<VSREtype>::TstMsgType, int vw) {
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
      [](int from, int to, FakeTMsgBuggyNetwork<VSREtype>::TstMsgType, int vw) {
        return (from == 4 && to != 0) || (from == 0 && to != 4); //from == 4 || from == 0;
      });
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

  // Make replica:4-0 non-isolated again
  buggynw.SetDecideFun(
      [](int from, int to, FakeTMsgBuggyNetwork<VSREtype>::TstMsgType, int vw) { return 0; });
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

  //
  // SPLIT BRAIN --------------------------------------------------
  // --------------------------------------------------------------
  // make replica:1-2 an island of network separate from the rest
  // --------------------------------------------------------------
  //

  buggynw.SetDecideFun(
      [](int from, int to, FakeTMsgBuggyNetwork<VSREtype>::TstMsgType, int vw) {
        if ((from == 1 && to != 2) || (from == 2 && to != 1))
          return true;
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
}

}
}
