#include <iostream>
#include <memory>
#include "float_value_factory.hh"
#include "sparse_matrix_value_generator.hh"

int main() {
    // example output of generators
    for (int j = 0; j < 5; j++) {
        std::shared_ptr<matrix_value_factory<float>> f = std::make_shared<float_value_factory>(-20, 20, 2137 + j/2);
        sparse_matrix_value_generator<float> gen(25, 25, 25, 2137 + j, f);
        std::cout << "\nh: " << gen.height() << " w: " << gen.width() << std::endl;
        for (int i = 0; gen.has_next(); i++) {
            auto mx_val = gen.next();
            std::cout << mx_val.val << " " << mx_val.i << " " << mx_val.j <<  std::endl;
        }
    }

    // generate matrix and upload to scylla

    // do multiplication

    // test result
    // if ( check(result, generator) ) {};

    return 0;
}