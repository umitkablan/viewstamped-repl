
enable_testing()

add_executable(
  core_test hasher.cpp core_impl_test.cpp core_test.cpp)

target_link_libraries(core_test
  PRIVATE gtest_main gmock)

include(GoogleTest)
gtest_discover_tests(core_test)

install(TARGETS core_test
  DESTINATION bin/)

