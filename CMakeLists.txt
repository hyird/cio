cmake_minimum_required(VERSION 3.20)
project(cio LANGUAGES CXX VERSION 0.1.0)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

if(NOT CMAKE_BUILD_TYPE AND NOT CMAKE_CONFIGURATION_TYPES)
  set(CMAKE_BUILD_TYPE RelWithDebInfo CACHE STRING "" FORCE)
endif()

# Consumers that add_subdirectory() this project should not inherit its tests,
# examples or install rules.
if(CMAKE_SOURCE_DIR STREQUAL CMAKE_CURRENT_SOURCE_DIR)
  set(CIO_IS_TOP_LEVEL ON)
else()
  set(CIO_IS_TOP_LEVEL OFF)
endif()

option(CIO_BUILD_TESTS "Build cio tests" ${CIO_IS_TOP_LEVEL})
option(CIO_BUILD_EXAMPLES "Build cio examples" ${CIO_IS_TOP_LEVEL})
option(CIO_BUILD_BENCHMARKS "Build cio benchmarks" ${CIO_IS_TOP_LEVEL})
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
  src/timer_handle.cpp
  src/reactor_common.cpp
  src/reactor_epoll.cpp
  src/blocking_pool.cpp
  src/dns.cpp
  src/fs.cpp
  src/signal.cpp
  src/runtime.cpp
  src/net.cpp
  src/pool.cpp
  src/process.cpp
  src/metrics.cpp
)

# The committed version.hpp must agree with project(VERSION); a mismatch means
# one of the two was bumped alone.
file(READ "${CMAKE_CURRENT_SOURCE_DIR}/include/cio/version.hpp" _cio_version_hpp)
foreach(_part MAJOR MINOR PATCH)
  string(REGEX MATCH "CIO_VERSION_${_part} ([0-9]+)" _m "${_cio_version_hpp}")
  set(_cio_hpp_${_part} "${CMAKE_MATCH_1}")
endforeach()
if(NOT "${_cio_hpp_MAJOR}.${_cio_hpp_MINOR}.${_cio_hpp_PATCH}" STREQUAL
   "${PROJECT_VERSION}")
  message(FATAL_ERROR
    "include/cio/version.hpp says "
    "${_cio_hpp_MAJOR}.${_cio_hpp_MINOR}.${_cio_hpp_PATCH} but project() says "
    "${PROJECT_VERSION}; update both.")
endif()

add_library(cio ${CIO_SOURCES})
add_library(cio::cio ALIAS cio)
set_target_properties(cio PROPERTIES
  VERSION ${PROJECT_VERSION}
  SOVERSION ${PROJECT_VERSION_MAJOR})

target_include_directories(cio PUBLIC
  $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
  $<INSTALL_INTERFACE:include>)
target_compile_features(cio PUBLIC cxx_std_20)
target_link_libraries(cio PUBLIC Threads::Threads)
# Select the library-only hidden alias of the default-visible cooperative-I/O
# TLS counter. Consumers keep the compiler's normal TLS model with libcio.so.
target_compile_definitions(cio PRIVATE CIO_BUILDING_LIBRARY=1)
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

# The TLS module is optional because it is the only part of the project with an
# external dependency. The core library must stay dependency-free.
option(CIO_TLS "Build the optional TLS module (requires OpenSSL)" OFF)
if(CIO_TLS)
  find_package(OpenSSL REQUIRED)
  add_library(cio_tls src/tls.cpp)
  add_library(cio::tls ALIAS cio_tls)
  target_link_libraries(cio_tls PUBLIC cio OpenSSL::SSL OpenSSL::Crypto)
  target_compile_features(cio_tls PUBLIC cxx_std_20)
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
  # test_tls only builds with the optional module that it exercises.
  if(NOT CIO_TLS)
    list(REMOVE_ITEM CIO_TEST_FILES test_tls.cpp)
  endif()
  foreach(test_file ${CIO_TEST_FILES})
    get_filename_component(test_name ${test_file} NAME_WE)
    cio_add_program(tests ${test_name})
    if(test_name STREQUAL "test_tls")
      target_link_libraries(${test_name} PRIVATE cio_tls)
    endif()
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

# ----------------------------------------------------------------- install ---
#
# Exported so a downstream can find_package(cio) and link cio::cio, instead of
# vendoring the source tree. Skipped when cio is consumed via add_subdirectory,
# where the parent project owns installation.
if(CIO_IS_TOP_LEVEL)
  include(GNUInstallDirs)
  include(CMakePackageConfigHelpers)

  set(CIO_INSTALL_TARGETS cio)
  if(CIO_TLS)
    list(APPEND CIO_INSTALL_TARGETS cio_tls)
  endif()

  install(TARGETS cio
    EXPORT cio-targets
    ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
    LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
    RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR})
  install(EXPORT cio-targets
    FILE cio-targets.cmake
    NAMESPACE cio::
    DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/cio)

  if(CIO_TLS)
    install(TARGETS cio_tls
      EXPORT cio-tls-targets
      ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
      LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR})
    install(EXPORT cio-tls-targets
      FILE cio-tls-targets.cmake
      NAMESPACE cio::
      DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/cio)
  endif()

  install(DIRECTORY include/cio
    DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}
    FILES_MATCHING PATTERN "*.hpp")

  configure_package_config_file(
    cmake/cio-config.cmake.in
    "${CMAKE_CURRENT_BINARY_DIR}/cio-config.cmake"
    INSTALL_DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/cio)
  write_basic_package_version_file(
    "${CMAKE_CURRENT_BINARY_DIR}/cio-config-version.cmake"
    VERSION ${PROJECT_VERSION}
    COMPATIBILITY SameMinorVersion)
  install(FILES
    "${CMAKE_CURRENT_BINARY_DIR}/cio-config.cmake"
    "${CMAKE_CURRENT_BINARY_DIR}/cio-config-version.cmake"
    DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/cio)

  install(FILES LICENSE
    DESTINATION ${CMAKE_INSTALL_DATAROOTDIR}/licenses/cio)
endif()
