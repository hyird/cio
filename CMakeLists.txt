cmake_minimum_required(VERSION 3.20)

project(
  CIO
  VERSION 0.1.0
  DESCRIPTION "Tokio 的 C++20 跨平台对等异步运行时"
  LANGUAGES CXX
)

option(CIO_BUILD_TESTS "构建 CIO 测试" ON)
option(CIO_BUILD_BENCHMARKS "构建 CIO、Asio 性能对比程序" OFF)
option(CIO_ENABLE_TOKIO_DIFFERENTIAL_TESTS "启用 Tokio 1.53.1 差分测试" OFF)
option(CIO_ENABLE_ASAN "启用 AddressSanitizer" OFF)
option(CIO_ENABLE_UBSAN "启用 UndefinedBehaviorSanitizer" OFF)
option(CIO_ENABLE_TSAN "启用 ThreadSanitizer" OFF)

if(CIO_ENABLE_TSAN AND (CIO_ENABLE_ASAN OR CIO_ENABLE_UBSAN))
  message(FATAL_ERROR "ThreadSanitizer 必须单独构建，不能与 ASan/UBSan 混用")
endif()

set(CIO_SANITIZER_COMPILE_OPTIONS)
set(CIO_SANITIZER_LINK_OPTIONS)
if(MSVC)
  if(CIO_ENABLE_UBSAN OR CIO_ENABLE_TSAN)
    message(FATAL_ERROR "MSVC 配置不支持 CIO 的 UBSan/TSan 选项")
  endif()
  if(CIO_ENABLE_ASAN)
    list(APPEND CIO_SANITIZER_COMPILE_OPTIONS /fsanitize=address)
    list(APPEND CIO_SANITIZER_LINK_OPTIONS /INCREMENTAL:NO)
  endif()
else()
  if(CIO_ENABLE_ASAN)
    list(APPEND CIO_SANITIZER_COMPILE_OPTIONS
      -fsanitize=address -fno-omit-frame-pointer
    )
    list(APPEND CIO_SANITIZER_LINK_OPTIONS -fsanitize=address)
  endif()
  if(CIO_ENABLE_UBSAN)
    list(APPEND CIO_SANITIZER_COMPILE_OPTIONS
      -fsanitize=undefined -fno-omit-frame-pointer
    )
    list(APPEND CIO_SANITIZER_LINK_OPTIONS -fsanitize=undefined)
  endif()
  if(CIO_ENABLE_TSAN)
    list(APPEND CIO_SANITIZER_COMPILE_OPTIONS
      -fsanitize=thread -fno-omit-frame-pointer
    )
    list(APPEND CIO_SANITIZER_LINK_OPTIONS -fsanitize=thread)
  endif()
endif()

add_library(cio STATIC
  src/runtime/blocking_pool.cpp
  src/runtime/runtime.cpp
  src/sync/semaphore.cpp
  src/time/time.cpp
  src/time/timer_driver.cpp
)
add_library(cio::cio ALIAS cio)

target_include_directories(cio
  PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
    $<INSTALL_INTERFACE:include>
)
target_compile_features(cio PUBLIC cxx_std_20)
target_compile_options(cio PRIVATE ${CIO_SANITIZER_COMPILE_OPTIONS})
set_target_properties(cio PROPERTIES
  CXX_EXTENSIONS OFF
  POSITION_INDEPENDENT_CODE ON
)

if(MSVC)
  target_compile_options(cio PRIVATE
    /W4 /permissive- /EHsc /Zc:__cplusplus /utf-8
  )
else()
  target_compile_options(cio PRIVATE -Wall -Wextra -Wpedantic -Wconversion -Wshadow)
endif()

if(CIO_BUILD_TESTS)
  include(CTest)
  enable_testing()

  add_executable(cio_runtime_tests
    tests/runtime_foundation_tests.cpp
  )
  target_link_libraries(cio_runtime_tests PRIVATE cio::cio)
  target_compile_features(cio_runtime_tests PRIVATE cxx_std_20)
  target_compile_options(cio_runtime_tests PRIVATE
    ${CIO_SANITIZER_COMPILE_OPTIONS}
  )
  target_link_options(cio_runtime_tests PRIVATE ${CIO_SANITIZER_LINK_OPTIONS})
  set_target_properties(cio_runtime_tests PROPERTIES CXX_EXTENSIONS OFF)

  if(MSVC)
    target_compile_options(cio_runtime_tests PRIVATE
      /W4 /permissive- /EHsc /Zc:__cplusplus /utf-8
    )
  else()
    target_compile_options(cio_runtime_tests PRIVATE
      -Wall -Wextra -Wpedantic -Wconversion -Wshadow
    )
  endif()

  add_test(NAME cio.runtime.foundation COMMAND cio_runtime_tests)

  add_executable(cio_mutex_safety_tests
    tests/mutex_safety_tests.cpp
  )
  target_link_libraries(cio_mutex_safety_tests PRIVATE cio::cio)
  target_compile_features(cio_mutex_safety_tests PRIVATE cxx_std_20)
  target_compile_options(cio_mutex_safety_tests PRIVATE
    ${CIO_SANITIZER_COMPILE_OPTIONS}
  )
  target_link_options(cio_mutex_safety_tests PRIVATE
    ${CIO_SANITIZER_LINK_OPTIONS}
  )
  set_target_properties(cio_mutex_safety_tests PROPERTIES CXX_EXTENSIONS OFF)

  if(MSVC)
    target_compile_options(cio_mutex_safety_tests PRIVATE
      /W4 /permissive- /EHsc /Zc:__cplusplus /utf-8
    )
  else()
    target_compile_options(cio_mutex_safety_tests PRIVATE
      -Wall -Wextra -Wpedantic -Wconversion -Wshadow
    )
  endif()

  add_test(NAME cio.sync.mutex-safety COMMAND cio_mutex_safety_tests)

  add_executable(cio_rwlock_tests
    tests/rwlock_tests.cpp
  )
  target_link_libraries(cio_rwlock_tests PRIVATE cio::cio)
  target_compile_features(cio_rwlock_tests PRIVATE cxx_std_20)
  target_compile_options(cio_rwlock_tests PRIVATE
    ${CIO_SANITIZER_COMPILE_OPTIONS}
  )
  target_link_options(cio_rwlock_tests PRIVATE
    ${CIO_SANITIZER_LINK_OPTIONS}
  )
  set_target_properties(cio_rwlock_tests PROPERTIES CXX_EXTENSIONS OFF)

  if(MSVC)
    target_compile_options(cio_rwlock_tests PRIVATE
      /W4 /permissive- /EHsc /Zc:__cplusplus /utf-8
    )
  else()
    target_compile_options(cio_rwlock_tests PRIVATE
      -Wall -Wextra -Wpedantic -Wconversion -Wshadow
    )
  endif()

  add_test(NAME cio.sync.rwlock COMMAND cio_rwlock_tests)

  add_executable(cio_barrier_tests
    tests/barrier_tests.cpp
  )
  target_link_libraries(cio_barrier_tests PRIVATE cio::cio)
  target_compile_features(cio_barrier_tests PRIVATE cxx_std_20)
  target_compile_options(cio_barrier_tests PRIVATE
    ${CIO_SANITIZER_COMPILE_OPTIONS}
  )
  target_link_options(cio_barrier_tests PRIVATE
    ${CIO_SANITIZER_LINK_OPTIONS}
  )
  set_target_properties(cio_barrier_tests PROPERTIES CXX_EXTENSIONS OFF)

  if(MSVC)
    target_compile_options(cio_barrier_tests PRIVATE
      /W4 /permissive- /EHsc /Zc:__cplusplus /utf-8
    )
  else()
    target_compile_options(cio_barrier_tests PRIVATE
      -Wall -Wextra -Wpedantic -Wconversion -Wshadow
    )
  endif()

  add_test(NAME cio.sync.barrier COMMAND cio_barrier_tests)

  add_executable(cio_once_cell_tests
    tests/once_cell_tests.cpp
  )
  target_link_libraries(cio_once_cell_tests PRIVATE cio::cio)
  target_compile_features(cio_once_cell_tests PRIVATE cxx_std_20)
  target_compile_options(cio_once_cell_tests PRIVATE
    ${CIO_SANITIZER_COMPILE_OPTIONS}
  )
  target_link_options(cio_once_cell_tests PRIVATE
    ${CIO_SANITIZER_LINK_OPTIONS}
  )
  set_target_properties(cio_once_cell_tests PROPERTIES CXX_EXTENSIONS OFF)

  if(MSVC)
    target_compile_options(cio_once_cell_tests PRIVATE
      /W4 /permissive- /EHsc /Zc:__cplusplus /utf-8
    )
  else()
    target_compile_options(cio_once_cell_tests PRIVATE
      -Wall -Wextra -Wpedantic -Wconversion -Wshadow
    )
  endif()

  add_test(NAME cio.sync.once-cell COMMAND cio_once_cell_tests)

  add_executable(cio_set_once_tests
    tests/set_once_tests.cpp
  )
  target_link_libraries(cio_set_once_tests PRIVATE cio::cio)
  target_compile_features(cio_set_once_tests PRIVATE cxx_std_20)
  target_compile_options(cio_set_once_tests PRIVATE
    ${CIO_SANITIZER_COMPILE_OPTIONS}
  )
  target_link_options(cio_set_once_tests PRIVATE
    ${CIO_SANITIZER_LINK_OPTIONS}
  )
  set_target_properties(cio_set_once_tests PROPERTIES CXX_EXTENSIONS OFF)

  if(MSVC)
    target_compile_options(cio_set_once_tests PRIVATE
      /W4 /permissive- /EHsc /Zc:__cplusplus /utf-8
    )
  else()
    target_compile_options(cio_set_once_tests PRIVATE
      -Wall -Wextra -Wpedantic -Wconversion -Wshadow
    )
  endif()

  add_test(NAME cio.sync.set-once COMMAND cio_set_once_tests)

  add_executable(cio_oneshot_tests
    tests/oneshot_tests.cpp
  )
  target_link_libraries(cio_oneshot_tests PRIVATE cio::cio)
  target_compile_features(cio_oneshot_tests PRIVATE cxx_std_20)
  target_compile_options(cio_oneshot_tests PRIVATE
    ${CIO_SANITIZER_COMPILE_OPTIONS}
  )
  target_link_options(cio_oneshot_tests PRIVATE
    ${CIO_SANITIZER_LINK_OPTIONS}
  )
  set_target_properties(cio_oneshot_tests PROPERTIES CXX_EXTENSIONS OFF)

  if(MSVC)
    target_compile_options(cio_oneshot_tests PRIVATE
      /W4 /permissive- /EHsc /Zc:__cplusplus /utf-8
    )
  else()
    target_compile_options(cio_oneshot_tests PRIVATE
      -Wall -Wextra -Wpedantic -Wconversion -Wshadow
    )
  endif()

  add_test(NAME cio.sync.oneshot COMMAND cio_oneshot_tests)

  add_executable(cio_mpsc_tests
    tests/mpsc_tests.cpp
  )
  target_link_libraries(cio_mpsc_tests PRIVATE cio::cio)
  target_compile_features(cio_mpsc_tests PRIVATE cxx_std_20)
  target_compile_options(cio_mpsc_tests PRIVATE
    ${CIO_SANITIZER_COMPILE_OPTIONS}
  )
  target_link_options(cio_mpsc_tests PRIVATE
    ${CIO_SANITIZER_LINK_OPTIONS}
  )
  set_target_properties(cio_mpsc_tests PROPERTIES CXX_EXTENSIONS OFF)

  if(MSVC)
    target_compile_options(cio_mpsc_tests PRIVATE
      /W4 /permissive- /EHsc /Zc:__cplusplus /utf-8
    )
  else()
    target_compile_options(cio_mpsc_tests PRIVATE
      -Wall -Wextra -Wpedantic -Wconversion -Wshadow
    )
  endif()

  add_test(NAME cio.sync.mpsc COMMAND cio_mpsc_tests)

  add_executable(cio_mpsc_unbounded_tests
    tests/mpsc_unbounded_tests.cpp
  )
  target_link_libraries(cio_mpsc_unbounded_tests PRIVATE cio::cio)
  target_compile_features(cio_mpsc_unbounded_tests PRIVATE cxx_std_20)
  target_compile_options(cio_mpsc_unbounded_tests PRIVATE
    ${CIO_SANITIZER_COMPILE_OPTIONS}
  )
  target_link_options(cio_mpsc_unbounded_tests PRIVATE
    ${CIO_SANITIZER_LINK_OPTIONS}
  )
  set_target_properties(cio_mpsc_unbounded_tests
    PROPERTIES CXX_EXTENSIONS OFF
  )

  if(MSVC)
    target_compile_options(cio_mpsc_unbounded_tests PRIVATE
      /W4 /permissive- /EHsc /Zc:__cplusplus /utf-8
    )
  else()
    target_compile_options(cio_mpsc_unbounded_tests PRIVATE
      -Wall -Wextra -Wpedantic -Wconversion -Wshadow
    )
  endif()

  add_test(NAME cio.sync.mpsc-unbounded
    COMMAND cio_mpsc_unbounded_tests
  )

  add_executable(cio_watch_tests
    tests/watch_tests.cpp
  )
  target_link_libraries(cio_watch_tests PRIVATE cio::cio)
  target_compile_features(cio_watch_tests PRIVATE cxx_std_20)
  target_compile_options(cio_watch_tests PRIVATE
    ${CIO_SANITIZER_COMPILE_OPTIONS}
  )
  target_link_options(cio_watch_tests PRIVATE
    ${CIO_SANITIZER_LINK_OPTIONS}
  )
  set_target_properties(cio_watch_tests PROPERTIES CXX_EXTENSIONS OFF)

  if(MSVC)
    target_compile_options(cio_watch_tests PRIVATE
      /W4 /permissive- /EHsc /Zc:__cplusplus /utf-8
    )
  else()
    target_compile_options(cio_watch_tests PRIVATE
      -Wall -Wextra -Wpedantic -Wconversion -Wshadow
    )
  endif()

  add_test(NAME cio.sync.watch COMMAND cio_watch_tests)

  add_executable(cio_broadcast_tests
    tests/broadcast_tests.cpp
  )
  target_link_libraries(cio_broadcast_tests PRIVATE cio::cio)
  target_compile_features(cio_broadcast_tests PRIVATE cxx_std_20)
  target_compile_options(cio_broadcast_tests PRIVATE
    ${CIO_SANITIZER_COMPILE_OPTIONS}
  )
  target_link_options(cio_broadcast_tests PRIVATE
    ${CIO_SANITIZER_LINK_OPTIONS}
  )
  set_target_properties(cio_broadcast_tests PROPERTIES CXX_EXTENSIONS OFF)

  if(MSVC)
    target_compile_options(cio_broadcast_tests PRIVATE
      /W4 /WX /permissive- /EHsc /Zc:__cplusplus /utf-8
    )
  else()
    target_compile_options(cio_broadcast_tests PRIVATE
      -Wall -Wextra -Wpedantic -Wconversion -Wshadow -Werror
    )
  endif()

  add_test(NAME cio.sync.broadcast COMMAND cio_broadcast_tests)

  add_executable(cio_io_tests
    tests/io_tests.cpp
  )
  target_link_libraries(cio_io_tests PRIVATE cio::cio)
  target_compile_features(cio_io_tests PRIVATE cxx_std_20)
  target_compile_options(cio_io_tests PRIVATE
    ${CIO_SANITIZER_COMPILE_OPTIONS}
  )
  target_link_options(cio_io_tests PRIVATE
    ${CIO_SANITIZER_LINK_OPTIONS}
  )
  set_target_properties(cio_io_tests PROPERTIES CXX_EXTENSIONS OFF)

  if(MSVC)
    target_compile_options(cio_io_tests PRIVATE
      /W4 /WX /permissive- /EHsc /Zc:__cplusplus /utf-8
    )
  else()
    target_compile_options(cio_io_tests PRIVATE
      -Wall -Wextra -Wpedantic -Wconversion -Wshadow -Werror
    )
  endif()

  add_test(NAME cio.io.memory COMMAND cio_io_tests)

  add_executable(cio_io_exact_tests
    tests/io_exact_tests.cpp
  )
  target_link_libraries(cio_io_exact_tests PRIVATE cio::cio)
  target_compile_features(cio_io_exact_tests PRIVATE cxx_std_20)
  target_compile_options(cio_io_exact_tests PRIVATE
    ${CIO_SANITIZER_COMPILE_OPTIONS}
  )
  target_link_options(cio_io_exact_tests PRIVATE
    ${CIO_SANITIZER_LINK_OPTIONS}
  )
  set_target_properties(cio_io_exact_tests PROPERTIES CXX_EXTENSIONS OFF)

  if(MSVC)
    target_compile_options(cio_io_exact_tests PRIVATE
      /W4 /WX /permissive- /EHsc /Zc:__cplusplus /utf-8
    )
  else()
    target_compile_options(cio_io_exact_tests PRIVATE
      -Wall -Wextra -Wpedantic -Wconversion -Wshadow -Werror
    )
  endif()

  add_test(NAME cio.io.exact COMMAND cio_io_exact_tests)

  add_executable(cio_composed_poll_tests
    tests/composed_poll_tests.cpp
  )
  target_link_libraries(cio_composed_poll_tests PRIVATE cio::cio)
  target_compile_features(cio_composed_poll_tests PRIVATE cxx_std_20)
  target_compile_options(cio_composed_poll_tests PRIVATE
    ${CIO_SANITIZER_COMPILE_OPTIONS}
  )
  target_link_options(cio_composed_poll_tests PRIVATE
    ${CIO_SANITIZER_LINK_OPTIONS}
  )
  set_target_properties(cio_composed_poll_tests PROPERTIES
    CXX_EXTENSIONS OFF
  )

  if(MSVC)
    target_compile_options(cio_composed_poll_tests PRIVATE
      /W4 /WX /permissive- /EHsc /Zc:__cplusplus /utf-8
    )
  else()
    target_compile_options(cio_composed_poll_tests PRIVATE
      -Wall -Wextra -Wpedantic -Wconversion -Wshadow -Werror
    )
  endif()

  add_test(
    NAME cio.runtime.composed-poll
    COMMAND cio_composed_poll_tests
  )

  add_executable(cio_poll_native_operation_tests
    tests/poll_native_operation_tests.cpp
  )
  target_link_libraries(cio_poll_native_operation_tests PRIVATE cio::cio)
  target_compile_features(cio_poll_native_operation_tests
    PRIVATE cxx_std_20
  )
  target_compile_options(cio_poll_native_operation_tests PRIVATE
    ${CIO_SANITIZER_COMPILE_OPTIONS}
  )
  target_link_options(cio_poll_native_operation_tests PRIVATE
    ${CIO_SANITIZER_LINK_OPTIONS}
  )
  set_target_properties(cio_poll_native_operation_tests PROPERTIES
    CXX_EXTENSIONS OFF
  )

  if(MSVC)
    target_compile_options(cio_poll_native_operation_tests PRIVATE
      /W4 /WX /permissive- /EHsc /Zc:__cplusplus /utf-8
    )
  else()
    target_compile_options(cio_poll_native_operation_tests PRIVATE
      -Wall -Wextra -Wpedantic -Wconversion -Wshadow -Werror
    )
  endif()

  add_test(
    NAME cio.runtime.poll-native-operation
    COMMAND cio_poll_native_operation_tests
  )

  add_executable(cio_io_operation_tests
    tests/io_operation_tests.cpp
  )
  target_link_libraries(cio_io_operation_tests PRIVATE cio::cio)
  target_compile_features(cio_io_operation_tests PRIVATE cxx_std_20)
  target_compile_options(cio_io_operation_tests PRIVATE
    ${CIO_SANITIZER_COMPILE_OPTIONS}
  )
  target_link_options(cio_io_operation_tests PRIVATE
    ${CIO_SANITIZER_LINK_OPTIONS}
  )
  set_target_properties(cio_io_operation_tests PROPERTIES
    CXX_EXTENSIONS OFF
  )

  if(MSVC)
    target_compile_options(cio_io_operation_tests PRIVATE
      /W4 /WX /permissive- /EHsc /Zc:__cplusplus /utf-8
    )
  else()
    target_compile_options(cio_io_operation_tests PRIVATE
      -Wall -Wextra -Wpedantic -Wconversion -Wshadow -Werror
    )
  endif()

  add_test(
    NAME cio.io.operation
    COMMAND cio_io_operation_tests
  )

  add_executable(cio_copy_buffer_tests
    tests/copy_buffer_tests.cpp
  )
  target_link_libraries(cio_copy_buffer_tests PRIVATE cio::cio)
  target_compile_features(cio_copy_buffer_tests PRIVATE cxx_std_20)
  target_compile_options(cio_copy_buffer_tests PRIVATE
    ${CIO_SANITIZER_COMPILE_OPTIONS}
  )
  target_link_options(cio_copy_buffer_tests PRIVATE
    ${CIO_SANITIZER_LINK_OPTIONS}
  )
  set_target_properties(cio_copy_buffer_tests PROPERTIES
    CXX_EXTENSIONS OFF
  )

  if(MSVC)
    target_compile_options(cio_copy_buffer_tests PRIVATE
      /W4 /WX /permissive- /EHsc /Zc:__cplusplus /utf-8
    )
  else()
    target_compile_options(cio_copy_buffer_tests PRIVATE
      -Wall -Wextra -Wpedantic -Wconversion -Wshadow -Werror
    )
  endif()

  add_test(
    NAME cio.io.copy-buffer
    COMMAND cio_copy_buffer_tests
  )

  add_executable(cio_semantics_probe
    tests/differential/cio_semantics_probe.cpp
  )
  target_link_libraries(cio_semantics_probe PRIVATE cio::cio)
  target_compile_features(cio_semantics_probe PRIVATE cxx_std_20)
  target_compile_options(cio_semantics_probe PRIVATE
    ${CIO_SANITIZER_COMPILE_OPTIONS}
  )
  target_link_options(cio_semantics_probe PRIVATE
    ${CIO_SANITIZER_LINK_OPTIONS}
  )
  set_target_properties(cio_semantics_probe PROPERTIES CXX_EXTENSIONS OFF)

  if(MSVC)
    target_compile_options(cio_semantics_probe PRIVATE
      /W4 /permissive- /EHsc /Zc:__cplusplus /utf-8 /bigobj
    )
  else()
    target_compile_options(cio_semantics_probe PRIVATE
      -Wall -Wextra -Wpedantic -Wconversion -Wshadow
    )
  endif()

  add_test(NAME cio.semantics.probe COMMAND cio_semantics_probe)

  find_package(Python3 QUIET COMPONENTS Interpreter)
  if(Python3_Interpreter_FOUND)
    add_test(
      NAME cio.contract.no_raw_pointers
      COMMAND
        ${Python3_EXECUTABLE}
        ${CMAKE_CURRENT_SOURCE_DIR}/tools/check_core_contract.py
        ${CMAKE_CURRENT_SOURCE_DIR}
    )
  endif()

  if(CIO_ENABLE_TOKIO_DIFFERENTIAL_TESTS)
    if(NOT Python3_Interpreter_FOUND)
      message(FATAL_ERROR "Tokio 差分测试需要 Python3")
    endif()
    find_program(CIO_CARGO_EXECUTABLE cargo REQUIRED)

    add_test(
      NAME cio.differential.tokio-1.53.1
      COMMAND
        ${Python3_EXECUTABLE}
        ${CMAKE_CURRENT_SOURCE_DIR}/tests/differential/compare_semantics.py
        --cio $<TARGET_FILE:cio_semantics_probe>
        --cargo ${CIO_CARGO_EXECUTABLE}
        --manifest
          ${CMAKE_CURRENT_SOURCE_DIR}/tests/differential/tokio-reference/Cargo.toml
        --target-dir ${CMAKE_CURRENT_BINARY_DIR}/tokio-reference-target
    )
  endif()
endif()

if(CIO_BUILD_BENCHMARKS)
  add_executable(cio_benchmark
    benchmarks/cio_benchmark.cpp
  )
  target_link_libraries(cio_benchmark PRIVATE cio::cio)
  target_compile_features(cio_benchmark PRIVATE cxx_std_20)
  set_target_properties(cio_benchmark PROPERTIES CXX_EXTENSIONS OFF)

  find_package(asio CONFIG REQUIRED)
  add_executable(cio_asio_benchmark
    benchmarks/asio_benchmark.cpp
  )
  target_link_libraries(cio_asio_benchmark PRIVATE asio::asio)
  target_compile_features(cio_asio_benchmark PRIVATE cxx_std_20)
  set_target_properties(cio_asio_benchmark PROPERTIES CXX_EXTENSIONS OFF)
  if(WIN32)
    target_compile_definitions(cio_asio_benchmark PRIVATE
      _WIN32_WINNT=0x0A00
    )
  endif()

  foreach(CIO_BENCHMARK_TARGET IN ITEMS cio_benchmark cio_asio_benchmark)
    if(MSVC)
      target_compile_options(${CIO_BENCHMARK_TARGET} PRIVATE
        /W4 /permissive- /EHsc /Zc:__cplusplus /utf-8
      )
    else()
      target_compile_options(${CIO_BENCHMARK_TARGET} PRIVATE
        -Wall -Wextra -Wpedantic -Wconversion -Wshadow
      )
    endif()
  endforeach()
endif()
