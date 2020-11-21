#include "float_value_factory.hh"

float_value_factory::float_value_factory(float min, float max, int seed) {
    this->_min = min;
    this->_max = max;
    this->_rng = std::mt19937(seed);
    this->_dist = std::uniform_real_distribution<float>(min, max);
}

float float_value_factory::next() {
    return _dist(_rng);
}