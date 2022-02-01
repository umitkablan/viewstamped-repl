#include "hasher.hpp"

namespace vsrepl
{

std::size_t mergeLogsHashes(LogsIterTyp beg, LogsIterTyp end, std::size_t inithash)
{
  for (; beg != end; ++beg) {
    auto h = std::hash<int> {}(beg->first);
    inithash ^= (h << 1);
    // inithash ^= (1 << (beg.first % 17));
    h = std::hash<MsgClientOp>{}(beg->second);
    inithash ^= (h << 1);
  }
  return inithash;
}

}
