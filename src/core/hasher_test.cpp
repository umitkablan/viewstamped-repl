#include "hasher.hpp"

#include <gmock/gmock.h>
#include <gtest/gtest.h>

namespace vsrepl
{
namespace test
{

TEST(HasherTest, BasicHashAndMerge_Repeating)
{
  std::vector<std::pair<int, MsgClientOp>> vv {
    { 0, { 1, "x=3", 1 } },
    { 2, { 1, "x=4", 2 } },
    { 3, { 2, "y=0", 1 } },
    { 4, { 1, "x=-1", 3 } },
  };
  const auto hAll = mergeLogsHashes(vv.begin(), vv.end());
  const auto h0 = mergeLogsHashes(vv.begin(), vv.begin() + 2);
  const auto h2 = mergeLogsHashes(vv.begin() + 2, vv.begin() + 3, h0);
  const auto h3 = mergeLogsHashes(vv.begin() + 3, vv.begin() + 4, h2);

  ASSERT_EQ(hAll, h3);
  // below values change depending on (libc++) platform - fails during PR validation
  // ASSERT_EQ(std::size_t(10782676624795537932ull), h0);
  // ASSERT_EQ(std::size_t(11415403314268682002ull), h2);
  // ASSERT_EQ(std::size_t(15084498811304813772ull), h3);
}

}
}
