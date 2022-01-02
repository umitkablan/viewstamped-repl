#ifndef VSTAMPED_REPL_UTIL_H
#define VSTAMPED_REPL_UTIL_H

#include <iostream>
#include <mutex>

template <typename... Args>
void Print0();

template <>
void Print0<>() {}

template <typename T, typename... Args>
void Print0(const T& a, Args... args)
{
  std::cout << a;
  Print0(std::forward<Args>(args)...);
}

template <typename... Args>
void PrintSync(Args... args)
{
  extern std::mutex cout_mtx;

  std::lock_guard<std::mutex> lck(cout_mtx);
  Print0(std::forward<Args>(args)...);
}

#endif
