cmake_minimum_required(VERSION 3.20)
project(stdexec_affine VERSION 0.2.0 LANGUAGES CXX)

include(CTest)

add_library(stdexec_affine INTERFACE)
add_library(stdexec_affine::affine ALIAS stdexec_affine)
target_compile_features(stdexec_affine INTERFACE cxx_std_20)
target_include_directories(stdexec_affine INTERFACE
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
    $<INSTALL_INTERFACE:include>)

# The library is built on stdexec. Use the parent project's STDEXEC::stdexec target if it has one, otherwise a stdexec
# checkout given here (tested revision: NVIDIA/stdexec fb944c333ae00d11cc0c56e1fb9b2bce04b5e162).
set(STDEXEC_AFFINE_STDEXEC_SOURCE_DIR "" CACHE PATH "Path to a stdexec checkout")
if(NOT TARGET STDEXEC::stdexec)
  if(NOT STDEXEC_AFFINE_STDEXEC_SOURCE_DIR)
    message(FATAL_ERROR "stdexec_affine needs stdexec: set STDEXEC_AFFINE_STDEXEC_SOURCE_DIR or provide STDEXEC::stdexec")
  endif()
  set(STDEXEC_BUILD_TESTS OFF)
  set(STDEXEC_BUILD_EXAMPLES OFF)
  add_subdirectory(${STDEXEC_AFFINE_STDEXEC_SOURCE_DIR}
                   ${CMAKE_CURRENT_BINARY_DIR}/stdexec-build
                   EXCLUDE_FROM_ALL)
endif()
target_link_libraries(stdexec_affine INTERFACE STDEXEC::stdexec)

option(STDEXEC_AFFINE_BUILD_TESTS "Build tests and examples" ${BUILD_TESTING})

if(STDEXEC_AFFINE_BUILD_TESTS)
  add_executable(stdexec_affine_task_scope_test tests/task_scope_test.cpp)
  target_link_libraries(stdexec_affine_task_scope_test PRIVATE stdexec_affine::affine)
  target_include_directories(stdexec_affine_task_scope_test PRIVATE tests)
  add_test(NAME task_scope_test COMMAND stdexec_affine_task_scope_test)

  foreach(example round_trip nested)
    add_executable(stdexec_affine_example_${example} examples/${example}.cpp)
    target_link_libraries(stdexec_affine_example_${example} PRIVATE stdexec_affine::affine)
    target_include_directories(stdexec_affine_example_${example} PRIVATE tests)
    add_test(NAME example_${example} COMMAND stdexec_affine_example_${example})
  endforeach()
endif()
