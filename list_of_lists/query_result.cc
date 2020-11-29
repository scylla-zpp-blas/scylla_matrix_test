#include "query_result.hh"

query_result::query_result(const CassResult *res): _result(res), _iterator(cass_iterator_from_result(res)) {}

bool query_result::next_row() {
    if (!cass_iterator_next(_iterator)) return false;

    _row = cass_iterator_get_row(_iterator);
    return true;
}

const CassValue *query_result::get_column(const std::string& name) {
    return cass_row_get_column_by_name(_row, name.c_str());
}

query_result::~query_result() {
    cass_iterator_free(_iterator);
    cass_result_free(_result);
}
