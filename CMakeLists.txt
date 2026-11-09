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
# checkout given here, otherwise fetch the tested revision.
set(STDEXEC_AFFINE_STDEXEC_SOURCE_DIR "" CACHE PATH "Path to a stdexec checkout (empty: fetch the tested revision)")
if(NOT TARGET STDEXEC::stdexec)
  set(STDEXEC_BUILD_TESTS OFF)
  set(STDEXEC_BUILD_EXAMPLES OFF)
  if(STDEXEC_AFFINE_STDEXEC_SOURCE_DIR)
    add_subdirectory(${STDEXEC_AFFINE_STDEXEC_SOURCE_DIR}
                     ${CMAKE_CURRENT_BINARY_DIR}/stdexec-build
                     EXCLUDE_FROM_ALL)
  else()
    include(FetchContent)
    FetchContent_Declare(stdexec
      GIT_REPOSITORY https://github.com/NVIDIA/stdexec.git
      GIT_TAG fb944c333ae00d11cc0c56e1fb9b2bce04b5e162)
    FetchContent_MakeAvailable(stdexec)
  endif()
endif()
target_link_libraries(stdexec_affine INTERFACE STDEXEC::stdexec)

option(STDEXEC_AFFINE_BUILD_TESTS "Build the tests" ${BUILD_TESTING})
option(STDEXEC_AFFINE_BUILD_EXAMPLES "Build the examples" ${STDEXEC_AFFINE_BUILD_TESTS})

if(STDEXEC_AFFINE_BUILD_TESTS)
  foreach(test task_scope round_trip nested)
    add_executable(stdexec_affine_${test}_test tests/${test}_test.cpp)
    target_link_libraries(stdexec_affine_${test}_test PRIVATE stdexec_affine::affine)
    target_include_directories(stdexec_affine_${test}_test PRIVATE tests)
    add_test(NAME ${test}_test COMMAND stdexec_affine_${test}_test)
  endforeach()
endif()

# The interactive example needs a POSIX terminal.
if(STDEXEC_AFFINE_BUILD_EXAMPLES AND NOT WIN32)
  add_executable(spinner examples/spinner.cpp)
  target_link_libraries(spinner PRIVATE stdexec_affine::affine)
  target_compile_definitions(spinner PRIVATE WORDS_FILE="${CMAKE_CURRENT_SOURCE_DIR}/examples/words.txt")
endif()
