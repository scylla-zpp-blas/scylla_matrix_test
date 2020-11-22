cmake_minimum_required(VERSION 3.10)
project(scylla_matrix_test)

set(CMAKE_CXX_STANDARD 17)

include_directories(${CMAKE_SOURCE_DIR}/include)
link_directories(${CMAKE_SOURCE_DIR}/lib)

add_executable(scylla_matrix_test main.cpp matrix_value_generator.hh 
sparse_matrix_value_generator.hh matrix_value_factory.hh float_value_factory.cc 
float_value_factory.hh matrix_value.hh utils/requestor.cpp utils/requestor.h 
utils/connector.cpp utils/connector.h utils/int_math.cc utils/int_math.hh double_value_factory.cc double_value_factory.hh)

target_link_libraries(scylla_matrix_test cassandra)
