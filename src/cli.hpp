#ifndef VSREPL_CLI_INCLUDED_
#define VSREPL_CLI_INCLUDED_ 1

#include "msgs.hpp"

#include <chrono>
#include <mutex>
#include <thread>
#include <unordered_map>
#include <unordered_set>

namespace vsrepl
{

template <typename TMsgDispatcher>
class VSReplCli
{
public:
  enum class OpState : char {
    DoesntExist = 1,
    JustStarted = 7,
    Ongoing,
    Consumed,
  };

  VSReplCli(unsigned client_id, TMsgDispatcher& dp, int totreplicas,
      int timeout_tick = 5, std::chrono::milliseconds = std::chrono::milliseconds(100));

  void Start();
  void Stop();

  // returns a unique opID associated with opstr
  unsigned InitOp(const std::string& opstr);
  // idempotent operation, could be used to get status
  OpState StartOp(unsigned opID);
  // clean op's state iff operation is consumed, in order to free memory
  int DeleteOpID(unsigned opID);

  void ConsumeCliMsg(int from, const MsgPersistedCliOp&);
  void ConsumeReply(int from, const MsgLeaderRedirect&);
  void ConsumeReply(int from, const MsgPersistedCliOp&);

  void TimeTick();

private:
  struct opStruct {
    std::string str;
    OpState st;
    uint16_t tick_cnt;
    int lastrep;
    std::unordered_set<unsigned> recv_replicas_;
  };

  bool setView(int view);

private:
  const unsigned client_id_;
  TMsgDispatcher& dispatcher_;
  const int totreplicas_;
  const int consensus_min_;

  int last_view_;
  unsigned last_op_id_;
  std::unordered_map<unsigned, opStruct> opmap_;
  std::mutex opmap_mtx_;

  private:
    void timeTickThread();

    std::chrono::milliseconds tick_interval_;
    int timeout_tick_;
    volatile bool continue_timetick_;
    std::thread timeTickThread_;
};

}

#endif
