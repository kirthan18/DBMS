cmake_minimum_required(VERSION 3.3.2)
project(Btree)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11")

add_custom_target(Btree COMMAND make -C ~/cs564/DBMS/Btree
        CLION_EXE_DIR=~/cs564/DBMS/Btree)