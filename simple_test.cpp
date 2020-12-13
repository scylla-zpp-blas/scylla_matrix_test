#include "multiplicator.hh"
#include "float_value_factory.hh"
#include "sparse_matrix_value_generator.hh"
#include "compressed_sparse_row/compressed_sparse_row.hh"
#include "coordinate_list/coordinate_list.hh"
#include "dictionary_of_keys/dictionary_of_keys.hh"
#include <list>
#include "fmt/format.h"

#define BOOST_TEST_MAIN
#define BOOST_TEST_MODULE simple_test
#include <boost/test/unit_test.hpp>

const char *IP_ADDRESS = "172.19.0.2";

enum implementation {
    COORDINATE_LIST,
    COMPRESSED_SPARSE_ROW,
    DICTIONARY_OF_KEYS
};

std::list<matrix_value<float>> get_multiplied_vals(size_t dimension, size_t vals,
                                                   std::unique_ptr<multiplicator<float>> multiplicator, int seed) {
    std::shared_ptr factory = std::make_shared<float_value_factory>(0.0, 100.0, 0);

    multiplicator->load_matrix(sparse_matrix_value_generator<float>(dimension, dimension, vals, seed, factory));
    multiplicator->load_matrix(sparse_matrix_value_generator<float>(dimension, dimension, vals, 18121 * seed + 12, factory));

    multiplicator->multiply();

    std::list<matrix_value<float>> ret;

    for (int i = 1; i <= dimension; i++) {
        for (int j = 1; j <= dimension; j++) {
            auto result = multiplicator->get_result({i, j});
            if (result != 0) {
                ret.emplace_back(i, j, result);
            }
        }
    }

    return ret;
}

std::list<matrix_value<float>> get_result(implementation from, size_t dimension, size_t vals,
                                                          std::shared_ptr<connector> conn, int seed) {

    std::vector<std::unique_ptr<multiplicator<float>>> multiplicators;
    multiplicators.emplace_back(std::make_unique<COO<float>>(conn));
    multiplicators.emplace_back(std::make_unique<CSR<float>>(conn));
    multiplicators.emplace_back(std::make_unique<DOK<float>>(conn));

    auto &m = multiplicators[from];

    return get_multiplied_vals(dimension, vals, std::move(m), seed);
}
BOOST_TEST_SPECIALIZED_COLLECTION_COMPARE(std::list<matrix_value<float>>)

BOOST_AUTO_TEST_SUITE(simple_cross_antitest)
    BOOST_AUTO_TEST_CASE(test_fail) {
        std::shared_ptr<connector> conn = std::make_shared<connector>(IP_ADDRESS);
        auto vals_1 = get_multiplied_vals(10, 10, std::make_unique<COO<float>>(conn), 1);
        auto vals_2 = get_multiplied_vals(10, 10, std::make_unique<CSR<float>>(conn), 111);

        BOOST_TEST(vals_1 != vals_2);
    }
BOOST_AUTO_TEST_SUITE_END()

BOOST_AUTO_TEST_SUITE(simple_cross_test)
    BOOST_AUTO_TEST_CASE(test_all_implementations0) {
        std::shared_ptr<connector> conn = std::make_shared<connector>(IP_ADDRESS);
        auto _coo = get_result(COORDINATE_LIST, 3, 3, conn, 2);
        auto _csr = get_result(COMPRESSED_SPARSE_ROW, 3, 3, conn, 2);
        auto _dok = get_result(DICTIONARY_OF_KEYS, 3, 3, conn, 2);

        BOOST_TEST(_coo == _csr);;
        BOOST_TEST(_coo == _dok);
    }

    BOOST_AUTO_TEST_CASE(test_all_implementations1) {
        std::shared_ptr<connector> conn = std::make_shared<connector>(IP_ADDRESS);
        auto _coo = get_result(COORDINATE_LIST, 10, 10, conn, 1);
        auto _csr = get_result(COMPRESSED_SPARSE_ROW, 10, 10, conn, 1);
        auto _dok = get_result(DICTIONARY_OF_KEYS, 10, 10, conn, 1);

        BOOST_TEST(_coo == _csr);
        BOOST_TEST(_coo == _dok);
    }

    BOOST_AUTO_TEST_CASE(test_all_implementations2) {
        std::shared_ptr<connector> conn = std::make_shared<connector>(IP_ADDRESS);
        auto _coo = get_result(COORDINATE_LIST, 20, 20, conn, 3);
        auto _csr = get_result(COMPRESSED_SPARSE_ROW, 20, 20, conn, 3);
        auto _dok = get_result(DICTIONARY_OF_KEYS, 20, 20, conn, 3);

        BOOST_TEST(_coo == _csr);
        BOOST_TEST(_coo == _dok);
    }

    BOOST_AUTO_TEST_CASE(test_all_implementations3) {
        std::shared_ptr<connector> conn = std::make_shared<connector>(IP_ADDRESS);
        auto _coo = get_result(COORDINATE_LIST, 6, 30, conn, 42);
        auto _csr = get_result(COMPRESSED_SPARSE_ROW, 6, 30, conn, 42);
        auto _dok = get_result(DICTIONARY_OF_KEYS, 6, 30, conn, 42);

        BOOST_TEST(_coo == _csr);
        BOOST_TEST(_coo == _dok);
    }
BOOST_AUTO_TEST_SUITE_END()
