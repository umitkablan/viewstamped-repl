#ifndef VSREPL_LOGGER_INCLUDED_
#define VSREPL_LOGGER_INCLUDED_ 1

#include <spdlog/spdlog.h>

#include <utility>

namespace vsrepl
{

template <typename... Args>
void log_info(Args&&... args) {
  spdlog::info(std::forward<Args>(args)...);
}

}

#endif
