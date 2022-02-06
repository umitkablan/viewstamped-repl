cmake_minimum_required(VERSION 3.14)
project(viewstamped-repl VERSION 0.0.1 LANGUAGES CXX)

set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_VERBOSE_MAKEFILE ON)
set (CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -fno-omit-frame-pointer -fsanitize=address")

include(FetchContent)
FetchContent_Declare(
  googletest
  URL https://github.com/google/googletest/archive/609281088cfefc76f9d0ce82e1ff6c30cc3591e5.zip
)
# For Windows: Prevent overriding the parent project's compiler/linker settings
set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)
FetchContent_MakeAvailable(googletest)

include(ExternalProject)
ExternalProject_Add(SPDLOG
  URL https://github.com/gabime/spdlog/archive/refs/tags/v1.9.2.tar.gz
  CONFIGURE_COMMAND cmake -E echo "Skipping configure step."
  BUILD_COMMAND cmake -E echo "Skipping build step."
  INSTALL_COMMAND cmake -E echo "Skipping install step."
  # CMAKE_ARGS -DCMAKE_INSTALL_PREFIX=${CMAKE_BINARY_DIR}/SPDLOG_install
)
ExternalProject_Get_Property(SPDLOG source_dir)
set(spdlog_SRCDIR ${source_dir})


add_subdirectory(src)

