//
// Created by hayven on 21.11.2020.
//

#include "requestor.hh"

requestor::requestor(std::shared_ptr<connector> conn) : _conn(conn) {}

std::ostream& operator<<(std::ostream& os, requestor& r) {
    os << r._query.str();
    return os;
}

void requestor::send() {
    std::cerr << _query.str() << std::endl;

    _statement = cass_statement_new(_query.str().c_str(), 0);
    cass_statement_set_consistency(_statement, CASS_CONSISTENCY_QUORUM);

    _result_future = cass_session_execute(_conn->get_session(), _statement);

    if (cass_future_error_code(_result_future) == CASS_OK) {
        _result = cass_future_get_result(_result_future);
        _iterator = cass_iterator_from_result(_result);
    } else {
        throw std::runtime_error("Query error");
    }
}

bool requestor::next_row() {
    if (!cass_iterator_next(_iterator)) return false;

    _row = cass_iterator_get_row(_iterator);
    return true;
}

const CassValue* requestor::get_column(std::string name) {
    return cass_row_get_column_by_name(_row, name.c_str());
}

requestor::~requestor() {
    cass_iterator_free(_iterator);
    cass_result_free(_result);
    cass_future_free(_result_future);
    cass_statement_free(_statement);
}