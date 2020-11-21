#pragma once

#include <cstddef>

template <class V>
struct matrix_value {
    size_t i, j;
    V val;

    matrix_value(size_t i, size_t j, V val) : i(i), j(j), val(val) {}
};