cmake_minimum_required(VERSION 2.8.9)
project(kvstore)

include(cmake/FindLibUuid.cmake)

include_directories(include ${LIBUUID_INCLUDE_DIRS})
#add_definitions(-Wno-c++11-extensions -std=c++1y -pthread -g3 -fno-omit-frame-pointer)
add_definitions(-Wno-c++11-extensions -std=c++1y -pthread -O2 -DNDEBUG -g3 -fno-omit-frame-pointer)

find_package (Threads REQUIRED)
add_executable (run_tests test/test.cpp)
add_test (NAME test COMMAND run_tests)
target_link_libraries (run_tests ${CMAKE_THREAD_LIBS_INIT} ${LIBUUID_LIBRARIES})

add_executable(benchmark test/benchmark.cpp)
target_link_libraries (benchmark ${CMAKE_THREAD_LIBS_INIT} ${LIBUUID_LIBRARIES})
