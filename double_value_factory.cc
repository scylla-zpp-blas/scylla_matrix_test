#include "double_value_factory.hh"

double_value_factory::double_value_factory(double min, double max, int seed) {
    this->_min = min;
    this->_max = max;
    this->_rng = std::mt19937(seed);
    this->_dist = std::uniform_real_distribution<double>(min, max);
}

double double_value_factory::next() {
    return _dist(_rng);
}