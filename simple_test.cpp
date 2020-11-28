#include "multiplicator.hh"
#include "float_value_factory.hh"
#include "sparse_matrix_value_generator.hh"
#include "compressed_sparse_row/compressed_sparse_row.hh"
#include "coordinate_list/coordinate_list.hh"
#include <list>

std::list<matrix_value<float>> get_multiplied_vals(size_t dimension, size_t vals, std::unique_ptr<multiplicator<float>> multiplicator) {
    std::shared_ptr factory = std::make_shared<float_value_factory>(0.0, 100.0, 0);

    multiplicator->load_matrix(sparse_matrix_value_generator<float>(dimension, dimension, vals, 0, factory));
    multiplicator->load_matrix(sparse_matrix_value_generator<float>(dimension, dimension, vals, 1, factory));

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

int main(int argc, char* argv[]) {
    std::shared_ptr<connector> conn;

    try {
        conn = std::make_shared<connector>("172.17.0.2");
        std::cerr << "Connected" << std::endl;
    } catch (...) {
        std::cerr << "Connection error" << std::endl;
        exit(1);
    }

    auto vals_1 = get_multiplied_vals(100, 200, std::make_unique<COO<float>>(conn));
    auto vals_2 = get_multiplied_vals(100, 200, std::make_unique<CSR<float>>(conn));

    auto it_1 = vals_1.begin();
    auto it_2 = vals_2.begin();
    const double eps = 1e-2;

    while (it_1 != vals_1.end() && it_2 != vals_2.end()) {
        auto p1 = std::make_pair(it_1->i, it_1->j);
        auto p2 = std::make_pair(it_2->i, it_2->j);

        if (p1 == p2) {
            if (abs(it_1->val - it_2->val) > eps) {
                std::cout << "Wrong value! (" << it_1->i << ", " << it_1->j << ") = "
                          << it_1->val << " or " << it_2->val << "?" << std::endl;
            }
            it_1++;
            it_2++;
        } else {
            if (p1 < p2) {
                std::cout << "Spare entry in COO result: (" << it_1->i << ", " << it_1->j << ") = " << it_1->val
                          << std::endl;
                it_1++;
            }
            else {
                std::cout << "Spare entry in CSR result: (" << it_2->i << ", " << it_2->j << ") = " << it_2->val
                          << std::endl;
                it_2++;
            }
        }
    }

    return 0;
}