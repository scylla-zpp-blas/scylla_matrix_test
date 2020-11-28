cmake_minimum_required(VERSION 3.10)
project(scylla_matrix_test)

set(CMAKE_CXX_STANDARD 17)

include_directories(${CMAKE_SOURCE_DIR}/include)
link_directories(${CMAKE_SOURCE_DIR}/lib)

set(BASE_SRC
        matrix_value.hh)

set(GENERATOR_SRC
        matrix_value_generator.hh
        sparse_matrix_value_generator.hh
        matrix_value_factory.hh
        float_value_factory.hh
        float_value_factory.cc
        double_value_factory.hh
        double_value_factory.cc
        )

set(UTILS_SRC
        utils/connector.hh
        utils/connector.cc
        utils/int_math.hh
        utils/int_math.cc
        utils/requestor.hh
        utils/requestor.cc
        )

add_executable(scylla_matrix_test main.cc "${BASE_SRC}" "${GENERATOR_SRC}" "${UTILS_SRC}")
target_link_libraries(scylla_matrix_test cassandra)

add_executable(simple_test simple_test.cpp coordinate_list/coordinate_list.hh "${BASE_SRC}" "${GENERATOR_SRC}" "${UTILS_SRC}")
target_link_libraries(simple_test cassandra)

add_executable(dok_test dictionary_of_keys/dictionary_of_keys.cc "${BASE_SRC}" "${GENERATOR_SRC}" "${UTILS_SRC}")
target_link_libraries(dok_test cassandra)

