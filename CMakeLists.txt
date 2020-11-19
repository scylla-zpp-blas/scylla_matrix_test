cmake_minimum_required(VERSION 3.15)
project(scylla_matrix_test)

set(CMAKE_CXX_STANDARD 17)

add_executable(scylla_matrix_test main.cpp matrix_factory.cc matrix_factory.hh)