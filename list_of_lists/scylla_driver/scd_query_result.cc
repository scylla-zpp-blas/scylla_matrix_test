#include <utility>
#include "scd_query_result.hh"

scd_query_result::scd_query_result(const CassResult *res): _result(res), _iterator(cass_iterator_from_result(res)) {}

bool scd_query_result::next_row() {
    if (!cass_iterator_next(_iterator)) return false;

    _row = cass_iterator_get_row(_iterator);
    return true;
}

scd_query_result::~scd_query_result() {
    cass_iterator_free(_iterator);
    cass_result_free(_result);
}

scd_query_result::scd_query_result(scd_query_result &&other) noexcept :
    _result(std::exchange(other._result, nullptr)),
    _iterator(std::exchange(other._iterator, nullptr)),
    _row(std::exchange(other._row, nullptr)) {}

scd_query_result &scd_query_result::operator=(scd_query_result &&other) noexcept {
    std::swap(_result, other._result);
    std::swap(_iterator, other._iterator);
    std::swap(_row, other._row);
    return *this;
}

const CassValue *scd_query_result::get_column_raw(const std::string &name) {
    return cass_row_get_column_by_name(_row, name.c_str());
}


const CassValue *scd_query_result::get_column_raw(size_t index) {
    return cass_row_get_column(_row, index);
}

bool scd_query_result::is_column_null(const std::string &name) {
    return is_cass_value_null(get_column_raw(name));
}

bool scd_query_result::is_column_null(size_t index) {
    return is_cass_value_null(get_column_raw(index));
}

std::string scd_query_result::get_column_name(size_t index) {
    const char *name;
    size_t len;
    throw_on_cass_error(cass_result_column_name(_result, index, &name, &len));
    return std::string(name, len);
}

