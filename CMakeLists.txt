cmake_minimum_required(VERSION 3.15)
project(scylla_matrix_test)

set(CMAKE_CXX_STANDARD 17)

add_executable(scylla_matrix_test main.cpp matrix_value_generator.hh 
sparse_matrix_value_generator.hh matrix_value_factory.hh float_value_factory.cc 
float_value_factory.hh matrix_value.hh utils/connector.cpp utils/connector.h 
utils/requestor.cpp utils/requestor.h multiplicator.h)
