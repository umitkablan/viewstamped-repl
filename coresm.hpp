#ifndef TSTAMPED_CORESM_INCLUDED_
#define TSTAMPED_CORESM_INCLUDED_ 1

#include "msgs.hpp"

#include <optional>
#include <utility>
#include <variant>
#include <vector>

namespace vsrepl {

enum class Status : char {
  Normal,
  Change,
};

class VSREngineCore {
public:
  using PrepareMsgsType = std::vector<std::pair<int, MsgPrepare>>;
  using SVCMsgsType = std::vector<std::pair<int, MsgStartViewChange>>;

  VSREngineCore(int totreplicas, int replica);

  std::variant<std::monostate, PrepareMsgsType, SVCMsgsType>
      HealthTimeoutTicked();
  std::variant<std::monostate, std::pair<int, MsgDoViewChange>, VSREngineCore::SVCMsgsType>
      SVCReceived(int from, const MsgStartViewChange& svc);
  std::vector<std::pair<int, MsgStartView>>
      DVCReceived(int from, const MsgDoViewChange& dvc);
  int SVReceived(int from, const MsgStartView& svc);
  int PrepareReceived(const MsgPrepare& pr);

  int View() const { return view_; }
  int Leader() const { return view_ % totreplicas_; }
  Status GetStatus() const { return status_; }

  const int totreplicas_;
  const int replica_;

private:
  unsigned latest_healthtick_received_;
  unsigned healthcheck_tick_;

  /**/
  int view_;
  Status status_;

private:
  struct trackDups {
    trackDups(int totreplicas)
      : recv_replicas_(totreplicas * totreplicas, 0)
      , recv_views_(totreplicas, -1) {}
    std::vector<int8_t> recv_replicas_;
    std::vector<int> recv_views_;
  };
  trackDups trackDups_SVCs_;
  trackDups trackDups_DVCs_;

  std::pair<bool,int>
  checkDuplicate(trackDups&, int from, int view);
};

}
#endif
