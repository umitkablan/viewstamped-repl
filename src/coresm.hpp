#ifndef TSTAMPED_CORESM_INCLUDED_
#define TSTAMPED_CORESM_INCLUDED_ 1

#include "msgs.hpp"

#include <optional>
#include <utility>
#include <variant>
#include <vector>

namespace vsrepl {

class VSREngineCore {
public:
  using PrepareMsgsType = std::vector<std::pair<int, MsgPrepare>>;
  using SVCMsgsType = std::vector<std::pair<int, MsgStartViewChange>>;

  VSREngineCore(int totreplicas, int replica);

  std::variant<std::monostate, PrepareMsgsType, SVCMsgsType>
      HealthTimeoutTicked(bool has_sent_prepare=false);
  std::variant<std::monostate, std::pair<int, MsgDoViewChange>, VSREngineCore::SVCMsgsType>
      SVCReceived(int from, const MsgStartViewChange& svc);
  std::vector<std::pair<int, MsgStartView>>
      DVCReceived(int from, const MsgDoViewChange& dvc);
  int SVReceived(int from, const MsgStartView& svc);
  int PrepareReceived(const MsgPrepare& pr);

  int View() const { return view_; }
  int Leader() const { return view_ % totreplicas_; }
  Status GetStatus() const { return status_; }

};

}
#endif
