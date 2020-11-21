#pragma once

template<class V>
class matrix_value_factory {
public:
    virtual V next() = 0;
};
