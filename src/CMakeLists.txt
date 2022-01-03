
enable_testing()

add_executable(
    core_test coresm.cpp coresm_test.cpp core_impl_test.cpp core_test.cpp)

target_link_libraries(
  core_test gtest_main)
target_link_libraries(
  core_test gmock)

include(GoogleTest)
gtest_discover_tests(core_test)
