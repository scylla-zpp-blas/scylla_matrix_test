#include "multiplicator.hh"
#include "float_value_factory.hh"
#include "sparse_matrix_value_generator.hh"
#include "compressed_sparse_row/compressed_sparse_row.hh"
#include "coordinate_list/coordinate_list.hh"
#include "dictionary_of_keys/dictionary_of_keys.hh"
#include <list>

#define BOOST_TEST_MAIN
#define BOOST_TEST_MODULE simple_test
#include <boost/test/unit_test.hpp>

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

std::list<std::list<matrix_value<float>>> get_all_results(size_t dimension, size_t vals,
                                                          std::shared_ptr<connector> conn, int seed) {

    std::vector<std::unique_ptr<multiplicator<float>>> multiplicators;
    multiplicators.emplace_back(std::make_unique<COO<float>>(conn));
    multiplicators.emplace_back(std::make_unique<CSR<float>>(conn));
    multiplicators.emplace_back(std::make_unique<DOK<float>>(conn));

    std::list<std::list<matrix_value<float>>> results;
    for (auto &m : multiplicators) {
        results.push_back(get_multiplied_vals(dimension, vals, std::move(m), seed));
    }

    return results;
}
BOOST_TEST_SPECIALIZED_COLLECTION_COMPARE(std::list<matrix_value<float>>)

BOOST_AUTO_TEST_SUITE(simple_cross_antitest)
    BOOST_AUTO_TEST_CASE(coo_vs_csr_fail) {
        std::shared_ptr<connector> conn = std::make_shared<connector>("172.17.0.2");
        auto vals_1 = get_multiplied_vals(10, 10, std::make_unique<COO<float>>(conn), 1);
        auto vals_2 = get_multiplied_vals(10, 10, std::make_unique<CSR<float>>(conn), 111);

        BOOST_TEST(vals_1 != vals_2);
    }
BOOST_AUTO_TEST_SUITE_END()

BOOST_AUTO_TEST_SUITE(simple_cross_test)
    BOOST_AUTO_TEST_CASE(coo_vs_csr1) {
        std::shared_ptr<connector> conn = std::make_shared<connector>("172.17.0.2");
        auto results = get_all_results(10, 10, conn, 1);

        auto it_1 = results.begin();
        auto it_2 = ++results.begin();

        while (it_2 != results.end()) {
            BOOST_TEST(*it_1 == *it_2);
            it_1++;
            it_2++;
        }
    }

    BOOST_AUTO_TEST_CASE(coo_vs_csr2) {
        std::shared_ptr<connector> conn = std::make_shared<connector>("172.17.0.2");
        auto results = get_all_results(20, 20, conn, 3);

        auto it_1 = results.begin();
        auto it_2 = ++results.begin();

        while (it_2 != results.end()) {
            BOOST_TEST(*it_1 == *it_2);
            it_1++;
            it_2++;
        }
    }

    BOOST_AUTO_TEST_CASE(coo_vs_csr3) {
        std::shared_ptr<connector> conn = std::make_shared<connector>("172.17.0.2");
        auto results = get_all_results(6, 30, conn, 42);

        auto it_1 = results.begin();
        auto it_2 = ++results.begin();

        while (it_2 != results.end()) {
            BOOST_TEST(*it_1 == *it_2);
            it_1++;
            it_2++;
        }
    }
BOOST_AUTO_TEST_SUITE_END()