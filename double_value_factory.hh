#pragma once

#include <random>
#include "matrix_value_factory.hh"

class double_value_factory: public matrix_value_factory<double> {
private:
    std::mt19937 _rng;
    double _min, _max;
    std::uniform_real_distribution<double> _dist;

public:
    double_value_factory(double min, double max, int seed);

    double next() override;
};
