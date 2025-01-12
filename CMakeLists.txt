cmake_minimum_required(VERSION 3.10)

project(broker_message)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED True)

add_executable(server src/server.cpp)
add_executable(client src/client.cpp src/lib.hpp)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall -Wextra -pedantic")