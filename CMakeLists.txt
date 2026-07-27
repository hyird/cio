cmake_minimum_required(VERSION 3.20)
project(cio LANGUAGES CXX VERSION 0.1.0)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

if(NOT CMAKE_BUILD_TYPE AND NOT CMAKE_CONFIGURATION_TYPES)
  set(CMAKE_BUILD_TYPE RelWithDebInfo CACHE STRING "" FORCE)
endif()

option(CIO_BUILD_TESTS "Build cio tests" ON)
option(CIO_BUILD_EXAMPLES "Build cio examples" ON)
option(CIO_BUILD_BENCHMARKS "Build cio benchmarks" ON)
option(CIO_SANITIZE "Build with thread/address sanitizer (tsan|asan|none)" "none")

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
)

add_library(cio ${CIO_SOURCES})
add_library(cio::cio ALIAS cio)

target_include_directories(cio PUBLIC
  $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
  $<INSTALL_INTERFACE:include>)
target_compile_features(cio PUBLIC cxx_std_20)
target_link_libraries(cio PUBLIC Threads::Threads)
target_compile_options(cio PRIVATE
  -Wall -Wextra -Wpedantic -Wshadow -Wno-interference-size)

if(CIO_SANITIZE STREQUAL "tsan")
  target_compile_options(cio PUBLIC -fsanitize=thread -g -O1)
  target_link_options(cio PUBLIC -fsanitize=thread)
elseif(CIO_SANITIZE STREQUAL "asan")
  target_compile_options(cio PUBLIC -fsanitize=address,undefined -g -O1)
  target_link_options(cio PUBLIC -fsanitize=address,undefined)
endif()

function(cio_add_program dir name)
  add_executable(${name} ${dir}/${name}.cpp)
  target_link_libraries(${name} PRIVATE cio)
  target_compile_options(${name} PRIVATE -Wall -Wextra -Wno-interference-size)
endfunction()

if(CIO_BUILD_TESTS)
  enable_testing()
  file(GLOB CIO_TEST_FILES RELATIVE ${CMAKE_CURRENT_SOURCE_DIR}/tests ${CMAKE_CURRENT_SOURCE_DIR}/tests/*.cpp)
  foreach(test_file ${CIO_TEST_FILES})
    get_filename_component(test_name ${test_file} NAME_WE)
    cio_add_program(tests ${test_name})
    add_test(NAME ${test_name} COMMAND ${test_name})
    set_tests_properties(${test_name} PROPERTIES TIMEOUT 120)
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
