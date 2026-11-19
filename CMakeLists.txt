cmake_minimum_required(VERSION 3.14)
project(lockfreequeues LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

find_package(Threads REQUIRED)
enable_testing()

add_executable(spsc_test tests/spsc_test.cpp)
target_include_directories(spsc_test PRIVATE ${CMAKE_SOURCE_DIR})
target_link_libraries(spsc_test PRIVATE Threads::Threads)

add_executable(mpsc_test tests/mpsc_test.cpp)
target_include_directories(mpsc_test PRIVATE ${CMAKE_SOURCE_DIR})
target_link_libraries(mpsc_test PRIVATE Threads::Threads)

add_test(NAME spsc_test COMMAND spsc_test)
add_test(NAME mpsc_test COMMAND mpsc_test)
