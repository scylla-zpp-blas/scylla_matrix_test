cmake_minimum_required(VERSION 3.10)
project(scylla_matrix_test)
enable_testing()

set(CMAKE_CXX_STANDARD 17)

add_subdirectory(lib/fmt EXCLUDE_FROM_ALL)
add_subdirectory(lib/scylla_driver EXCLUDE_FROM_ALL)

include_directories(${CMAKE_SOURCE_DIR}/include)

find_package(Boost 1.60.0 COMPONENTS system filesystem unit_test_framework REQUIRED )
include_directories( ${Boost_INCLUDE_DIR} )
add_definitions(-DBOOST_TEST_DYN_LINK)

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

add_executable(simple_test simple_test.cc coordinate_list/coordinate_list.hh "${BASE_SRC}" "${GENERATOR_SRC}" "${UTILS_SRC}")
target_link_libraries(simple_test 
        scylla_modern_cpp_driver
        ${Boost_FILESYSTEM_LIBRARY}
        ${Boost_SYSTEM_LIBRARY}
        ${Boost_UNIT_TEST_FRAMEWORK_LIBRARY}
        pthread
        fmt::fmt
        )

add_executable(dok_test dictionary_of_keys/dictionary_of_keys.cc "${BASE_SRC}" "${GENERATOR_SRC}" "${UTILS_SRC}" dictionary_of_keys/dictionary_of_keys.cc)
target_link_libraries(dok_test scylla_modern_cpp_driver fmt::fmt)



add_executable(lil_cli list_of_lists/list_of_lists_cli.cc "${BASE_SRC}" "${GENERATOR_SRC}" "${UTILS_SRC}" list_of_lists/list_of_lists.hh list_of_lists/list_of_lists_wrapper.hh)
target_link_libraries(lil_cli PUBLIC scylla_modern_cpp_driver fmt::fmt)

add_test(NAME test1 COMMAND simple_test)
