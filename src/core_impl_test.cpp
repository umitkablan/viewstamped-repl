#include "core.cpp"

#include "ifaces.hpp"

#include <future>
#include <mutex>
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

template <typename ViewStampedReplEngine>
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

  void SetEnginesStart(std::vector<ViewStampedReplEngine*> engines)
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
      MsgStartViewResponse svr{ "failxd-13 network" };
      if (!ret) {
        std::lock_guard<std::mutex> lck(engines_mtxs_[to]);
        svr = engines_[to]->ConsumeMsg(from, sv);
      } else
        return ret;

      ret = callDecideSync(from, to, TstMsgType::StartViewResponse, sv.view);
      if (!ret) {
        std::lock_guard<std::mutex> lck(engines_mtxs_[from]);
        return engines_[from]->ConsumeReply(to, svr);
      }
      return ret;
    });
  }

  void SendMsg(int from, int to, const MsgPrepare& pr) override
  {
    enqueueTask(pts_, [from, to, pr, this]() {
      auto ret = callDecideSync(from, to, TstMsgType::Prepare, pr.view);
      MsgPrepareResponse presp { "err asdeee" };
      if (!ret) {
        std::lock_guard<std::mutex> lck(engines_mtxs_[to]);
        presp = engines_[to]->ConsumeMsg(from, pr);
      } else return ret;

      ret = callDecideSync(from, to, TstMsgType::PrepareResponse, pr.view);
      if (!ret) {
        std::lock_guard<std::mutex> lck(engines_mtxs_[from]);
        return engines_[from]->ConsumeReply(to, presp);
      }
      return ret;
    });
  }

private:
  int random_chose_border_;
  bool is_shuffle_;
  std::vector<ViewStampedReplEngine*> engines_;
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
      std::this_thread::sleep_for(std::chrono::milliseconds(5));
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

}

// initiate template for test integration
template class ViewstampedReplicationEngine<test::MockTMsgDispatcher, test::MockStateMachine>;
template class ViewstampedReplicationEngine<test::ParentMsgDispatcher, test::MockStateMachine>;

}