
enable_testing()

add_executable(
  core_test hasher.cpp hasher_test.cpp core_impl_test.cpp core_test.cpp cli_test.cpp
  integration_test.cpp)

target_include_directories(core_test
  PRIVATE ${spdlog_SRCDIR}/include ${CMAKE_SOURCE_DIR}/src)
target_link_libraries(core_test
  PRIVATE gtest_main gmock)

include(GoogleTest)
gtest_discover_tests(core_test)

install(TARGETS core_test
  DESTINATION bin/)

