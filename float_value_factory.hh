#pragma once

#include <random>
#include "matrix_value_factory.hh"

class float_value_factory: public matrix_value_factory<float> {
private:
    std::mt19937 _rng;
    float _min, _max;
    std::uniform_real_distribution<float> _dist;

public:
    float_value_factory(float min, float max, int seed);

    float next() override;
};
