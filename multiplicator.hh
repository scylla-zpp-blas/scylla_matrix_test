//
// Created by hayven on 21.11.2020.
//

#ifndef SCYLLA_MATRIX_TEST_MULTIPLICATOR_HH
#define SCYLLA_MATRIX_TEST_MULTIPLICATOR_HH

#include "matrix_value_generator.hh"
#include <utility>

/* Abstract API for specialized matrix multiplicators. */
template<typename T>
class multiplicator {

public:
    /* Load values in the matrix. This has to be called exactly twice before calling multiplication. */
    virtual void load_matrix(matrix_value_generator<T>&& gen) = 0;

    /* Multiplies two matrices loaded into Scylla with load_matrix. */
    virtual void multiply() = 0;

    /* Obtains the value in the multiplication result at (x; y) = (pos.first; pos.second) */
    virtual T get_result(std::pair<size_t, size_t> pos) = 0;

    virtual ~multiplicator() {};
};

#endif //SCYLLA_MATRIX_TEST_MULTIPLICATOR_HH
