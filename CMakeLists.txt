cmake_minimum_required(VERSION 3.20)
project(cio LANGUAGES CXX VERSION 0.0.1)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

if(NOT CMAKE_BUILD_TYPE AND NOT CMAKE_CONFIGURATION_TYPES)
  set(CMAKE_BUILD_TYPE RelWithDebInfo CACHE STRING "" FORCE)
endif()

option(CIO_BUILD_TESTS "Build cio tests" ON)
option(CIO_BUILD_EXAMPLES "Build cio examples" ON)
option(CIO_BUILD_BENCHMARKS "Build cio benchmarks" ON)
option(CIO_METRICS "Count scheduler/reactor events (see detail/metrics.hpp)" OFF)
# Not option(): that declares a BOOL, so the "none" default lands in the cache as
# OFF, no UI can offer the real choices, and a misspelled value silently
# produces an uninstrumented build that looks sanitized.
if(NOT DEFINED CIO_SANITIZE OR CIO_SANITIZE STREQUAL "OFF")
  set(_cio_sanitize "none")
else()
  set(_cio_sanitize "${CIO_SANITIZE}")
endif()
set(CIO_SANITIZE "${_cio_sanitize}" CACHE STRING
    "Build with a sanitizer (none|asan|tsan)" FORCE)
set_property(CACHE CIO_SANITIZE PROPERTY STRINGS none asan tsan)
unset(_cio_sanitize)
if(NOT CIO_SANITIZE MATCHES "^(none|asan|tsan)$")
  message(FATAL_ERROR
    "Invalid CIO_SANITIZE='${CIO_SANITIZE}'; expected none, asan, or tsan")
endif()

find_package(Threads REQUIRED)

if(NOT CMAKE_SYSTEM_NAME STREQUAL "Linux")
  message(FATAL_ERROR "cio targets Linux only (epoll).")
endif()

set(CIO_SOURCES
  src/scheduler.cpp
  src/timer.cpp
  src/reactor_common.cpp
  src/reactor_epoll.cpp
  src/blocking_pool.cpp
  src/runtime.cpp
  src/net.cpp
  src/metrics.cpp
)

add_library(cio ${CIO_SOURCES})
add_library(cio::cio ALIAS cio)

target_include_directories(cio PUBLIC
  $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
  $<INSTALL_INTERFACE:include>)
target_compile_features(cio PUBLIC cxx_std_20)
target_link_libraries(cio PUBLIC Threads::Threads)
target_compile_options(cio PRIVATE
  -Wall -Wextra -Wpedantic -Wshadow
  # GCC-only; clang reports it as an unknown warning once per TU.
  $<$<CXX_COMPILER_ID:GNU>:-Wno-interference-size>)

# PRIVATE, and load-bearing that it is: every CIO_METRIC() call site is in a
# .cpp file in this target. One in a header would make an inline function's
# body depend on a macro consumers do not define, which is an ODR violation
# that links cleanly and then misbehaves. runtime_metrics() is declared
# unconditionally and defined either way, so consumers need no flag to call it.
if(CIO_METRICS)
  target_compile_definitions(cio PRIVATE CIO_METRICS=1)
endif()

# Load-bearing, and PUBLIC because it has to reach every translation unit that
# awaits a task, not just the library's own.
#
# C++20 coroutines resume each other by symmetric transfer, which is only
# constant-stack if the compiler emits it as a tail call. GCC does that only
# with -foptimize-sibling-calls, which -O2/-O3/-Og imply but -O0 and -O1 do not
# — and without it every `for (;;) co_await subtask()` loop grows the stack
# until it overflows. Measured: stack overflow at -O0 and -O1, zero growth once
# this flag is added at any level. Clang emits a musttail and does not need it.
if(CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
  target_compile_options(cio PUBLIC -foptimize-sibling-calls)
endif()

# -UNDEBUG so assertions survive: a sanitizer build with the runtime's internal
# invariant checks compiled out is the one build where they would pay off most.
if(CIO_SANITIZE STREQUAL "tsan")
  target_compile_options(cio PUBLIC -fsanitize=thread -g -O1 -UNDEBUG)
  target_link_options(cio PUBLIC -fsanitize=thread)
elseif(CIO_SANITIZE STREQUAL "asan")
  target_compile_options(cio PUBLIC -fsanitize=address,undefined -g -O1 -UNDEBUG)
  target_link_options(cio PUBLIC -fsanitize=address,undefined)
endif()

function(cio_add_program dir name)
  add_executable(${name} ${dir}/${name}.cpp)
  target_link_libraries(${name} PRIVATE cio)
  target_compile_options(${name} PRIVATE
    -Wall -Wextra
    $<$<CXX_COMPILER_ID:GNU>:-Wno-interference-size>)
endfunction()

if(CIO_BUILD_TESTS)
  enable_testing()
  file(GLOB CIO_TEST_FILES RELATIVE ${CMAKE_CURRENT_SOURCE_DIR}/tests ${CMAKE_CURRENT_SOURCE_DIR}/tests/*.cpp)
  foreach(test_file ${CIO_TEST_FILES})
    get_filename_component(test_name ${test_file} NAME_WE)
    cio_add_program(tests ${test_name})
    add_test(NAME ${test_name} COMMAND ${test_name})
    set_tests_properties(${test_name} PROPERTIES TIMEOUT 300)
  endforeach()
endif()

if(CIO_BUILD_EXAMPLES)
  file(GLOB CIO_EXAMPLE_FILES RELATIVE ${CMAKE_CURRENT_SOURCE_DIR}/examples ${CMAKE_CURRENT_SOURCE_DIR}/examples/*.cpp)
  foreach(example_file ${CIO_EXAMPLE_FILES})
    get_filename_component(example_name ${example_file} NAME_WE)
    cio_add_program(examples ${example_name})
  endforeach()
endif()

if(CIO_BUILD_BENCHMARKS)
  file(GLOB CIO_BENCH_FILES RELATIVE ${CMAKE_CURRENT_SOURCE_DIR}/bench ${CMAKE_CURRENT_SOURCE_DIR}/bench/*.cpp)
  foreach(bench_file ${CIO_BENCH_FILES})
    get_filename_component(bench_name ${bench_file} NAME_WE)
    cio_add_program(bench ${bench_name})
  endforeach()
endif()
