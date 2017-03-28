cmake_minimum_required(VERSION 3.6)
project(SQL)

set(CMAKE_CXX_STANDARD 11)

add_compile_options(-l sqlite3)

set(SOURCE_FILES main.cpp)
add_executable(SQL ${SOURCE_FILES})
target_link_libraries(SQL LINK_PUBLIC sqlite3)