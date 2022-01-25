#ifndef VSREPL_HASHER_INCLUDED_
#define VSREPL_HASHER_INCLUDED_ 1

#include "msgs.hpp"


namespace std {

template <>
struct hash<vsrepl::MsgClientOp> {
  std::size_t operator()(const vsrepl::MsgClientOp& cliop) const noexcept
  {
    return cliop.hash();
  }
};

}


namespace vsrepl
{
using LogsIterTyp = std::vector<std::pair<int, MsgClientOp>>::const_iterator;

std::size_t mergeLogsHashes(LogsIterTyp beg, LogsIterTyp end, std::size_t inithash = 0);
}
#endif
