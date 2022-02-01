#ifndef VSREPL_UTIL_INCLUDED
#define VSREPL_UTIL_INCLUDED

#include <functional>
#include <utility>

namespace vsrepl
{

struct PairHasher {
  template <typename T1, typename T2>
  std::size_t operator()(const std::pair<T1,T2>& pp) const noexcept {
    const auto h1 = std::hash<T1>{}(pp.first);
    const auto h2 = std::hash<T2>{}(pp.second);
    return h1 ^ h2;
  }
};

}

#endif
