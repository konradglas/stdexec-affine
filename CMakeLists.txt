cmake_minimum_required(VERSION 3.20)
project(stdexec_affine VERSION 0.1.0 LANGUAGES CXX)

include(FetchContent)
include(CTest)

add_library(stdexec_affine INTERFACE)
add_library(stdexec_affine::affine ALIAS stdexec_affine)
target_compile_features(stdexec_affine INTERFACE cxx_std_20)
if(MSVC)
  target_compile_options(stdexec_affine INTERFACE /Zc:preprocessor)
endif()
target_include_directories(stdexec_affine INTERFACE
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
    $<INSTALL_INTERFACE:include>)

set(STDEXEC_AFFINE_STDEXEC_SOURCE_DIR "" CACHE PATH
    "Path to a stdexec checkout used for sender integration")
if(STDEXEC_AFFINE_STDEXEC_SOURCE_DIR)
  add_subdirectory(${STDEXEC_AFFINE_STDEXEC_SOURCE_DIR}
                   ${CMAKE_CURRENT_BINARY_DIR}/stdexec-build
                   EXCLUDE_FROM_ALL)
  target_link_libraries(stdexec_affine INTERFACE STDEXEC::stdexec)
endif()

option(STDEXEC_AFFINE_BUILD_TESTS "Build tests" ${BUILD_TESTING})

if(STDEXEC_AFFINE_BUILD_TESTS)
  FetchContent_Declare(
    doctest
    URL https://github.com/doctest/doctest/archive/refs/tags/v2.4.11.tar.gz
  )
  FetchContent_MakeAvailable(doctest)

  add_executable(stdexec_affine_tests tests/affine_task_test.cpp tests/stop_token_test.cpp)
  target_link_libraries(stdexec_affine_tests PRIVATE stdexec_affine::affine doctest::doctest)
  target_compile_features(stdexec_affine_tests PRIVATE cxx_std_20)
  if(STDEXEC_AFFINE_STDEXEC_SOURCE_DIR)
    target_compile_definitions(stdexec_affine_tests PRIVATE AFFINE_TEST_HAS_STDEXEC=1)
  endif()
  include(${doctest_SOURCE_DIR}/scripts/cmake/doctest.cmake)
  doctest_discover_tests(stdexec_affine_tests)
endif()

install(DIRECTORY include/ DESTINATION include)
install(TARGETS stdexec_affine EXPORT stdexec_affine_targets)
if(NOT STDEXEC_AFFINE_STDEXEC_SOURCE_DIR)
  install(EXPORT stdexec_affine_targets
          NAMESPACE stdexec_affine::
          DESTINATION lib/cmake/stdexec_affine)
endif()
