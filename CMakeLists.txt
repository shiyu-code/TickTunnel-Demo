cmake_minimum_required(VERSION 3.10)
project(multi_server_demo)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

find_package(Boost 1.70 REQUIRED COMPONENTS system thread chrono atomic)
find_package(Threads REQUIRED)

include_directories(
    E:/lib/websocketpp-0.8.2
    E:/lib
    E:/lib/sqlite3
    E:/lib/openssl-install/include
)

link_directories(E:/lib/openssl-install/lib)

add_library(sqlite3 STATIC E:/lib/sqlite3/sqlite3.c)
target_include_directories(sqlite3 PUBLIC E:/lib/sqlite3)

add_executable(server_a server_a.cpp)
add_executable(server_b server_b.cpp)

target_link_libraries(server_a PRIVATE
    Threads::Threads
    Boost::system Boost::thread Boost::chrono Boost::atomic
    libcrypto libssl crypt32
)

target_link_libraries(server_b PRIVATE
    Threads::Threads
    Boost::system Boost::thread Boost::chrono Boost::atomic
    sqlite3 libcrypto libssl crypt32
)