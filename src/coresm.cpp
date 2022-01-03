#include "coresm.hpp"

#include <iostream>
using std::cout;
using std::endl;

namespace vsrepl {

VSREngineCore::VSREngineCore(int totreplicas, int replica)
{
}

std::variant<std::monostate, std::pair<int, MsgDoViewChange>, VSREngineCore::SVCMsgsType>
VSREngineCore::SVCReceived(int from, const MsgStartViewChange& svc)
{
  std::variant<std::monostate, std::pair<int, MsgDoViewChange>, VSREngineCore::SVCMsgsType> ret;


  return ret;
}

std::vector<std::pair<int, MsgStartView>>
VSREngineCore::DVCReceived(int from, const MsgDoViewChange& dvc)
{
  std::vector<std::pair<int, MsgStartView>> ret;


  return ret;
}

int VSREngineCore::SVReceived(int from, const MsgStartView& svc)
{
  // cout << replica_ << ":" << view_ << " (SV) v:" << svc.view << endl;

  return 0;
}

int VSREngineCore::PrepareReceived(const MsgPrepare& pr)
{
  return 0;
}

}
