cmake_minimum_required(VERSION 3.22)
project(ex3_os)

set(CMAKE_CXX_STANDARD 14)

include_directories(.)

add_executable(ex3_os
        Barrier.cpp
        Barrier.h
        MapReduceClient.h
        MapReduceFramework.cpp
        MapReduceFramework.h
        test1-1_thread_1_process.cpp
        test4-1_thread_4_process.cpp
        SampleClient.cpp)
