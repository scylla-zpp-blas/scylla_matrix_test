#pragma once

#include <cstddef>

template<class V>
class matrix_generator {
public:
    struct matrix_value {
        size_t i, j;
        V val;
    };

    virtual bool has_next() = 0;

    virtual matrix_value next() = 0;

    virtual size_t height() = 0;

    virtual size_t width() = 0;
};
