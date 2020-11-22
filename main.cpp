#include <iostream>
#include <memory>
#include "double_value_factory.hh"
#include "sparse_matrix_value_generator.hh"

int main() {
    // example output of generators
    for (int j = 0; j < 1; j++) {
        // init matrix val factory in shared_ptr
        std::shared_ptr<matrix_value_factory<double>> f = std::make_shared<double_value_factory>(-20, 20, 2137 + j/2);
        // use value factory in matrix generator
        sparse_matrix_value_generator<double> gen(5, 5, 25, 2137 + j, f);
        // test
        std::cout << "\nh: " << gen.height() << " w: " << gen.width() << std::endl;
        auto prev_mx_val = gen.next();
        for (int i = 0; gen.has_next(); i++) {
            auto mx_val = gen.next();
            if (prev_mx_val.i > mx_val.i || (prev_mx_val.i == mx_val.i && prev_mx_val.j > mx_val.j)) {
                return 1;
            }
            prev_mx_val = mx_val;
            std::cout << mx_val.val << " " << mx_val.i << " " << mx_val.j <<  std::endl;
        }
    }

    // generate matrix and upload to scylla

    // do multiplication

    // test result
    // if ( check(result, generator) ) {};

    return 0;
}