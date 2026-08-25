cmake_minimum_required(VERSION 3.20)
project(stdexec_affine VERSION 0.1.0 LANGUAGES CXX)

include(FetchContent)
include(CTest)

add_library(stdexec_affine INTERFACE)
add_library(exec::affine ALIAS stdexec_affine)
target_compile_features(stdexec_affine INTERFACE cxx_std_20)
target_include_directories(stdexec_affine INTERFACE
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
    $<INSTALL_INTERFACE:include>)

option(STDEXEC_AFFINE_BUILD_TESTS "Build tests" ${BUILD_TESTING})

if(STDEXEC_AFFINE_BUILD_TESTS)
  FetchContent_Declare(
    doctest
    URL https://github.com/doctest/doctest/archive/refs/tags/v2.4.11.tar.gz
  )
  FetchContent_MakeAvailable(doctest)

  add_executable(stdexec_affine_tests tests/affine_task_test.cpp)
  target_link_libraries(stdexec_affine_tests PRIVATE exec::affine doctest::doctest)
  target_compile_features(stdexec_affine_tests PRIVATE cxx_std_20)
  include(${doctest_SOURCE_DIR}/scripts/cmake/doctest.cmake)
  doctest_discover_tests(stdexec_affine_tests)
endif()

install(DIRECTORY include/ DESTINATION include)
install(TARGETS stdexec_affine EXPORT stdexec_affine_targets)
install(EXPORT stdexec_affine_targets
        NAMESPACE exec::
        DESTINATION lib/cmake/stdexec_affine)
