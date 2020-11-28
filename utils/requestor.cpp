//
// Created by hayven on 21.11.2020.
//

#include "requestor.h"

requestor::requestor(std::shared_ptr<connector> conn) : conn(conn) {}

std::ostream& operator<<(std::ostream& os, requestor& r) {
    os << r.query.str();
    return os;
}

void requestor::send() {
    // std::cerr << query.str() << std::endl;

    statement = cass_statement_new(query.str().c_str(), 0);
    cass_statement_set_consistency(statement, CASS_CONSISTENCY_QUORUM);

    result_future = cass_session_execute(conn->get_session(), statement);

    if (cass_future_error_code(result_future) == CASS_OK) {
        result = cass_future_get_result(result_future);
        iterator = cass_iterator_from_result(result);
    } else {
        throw std::runtime_error("Query error");
    }
}

bool requestor::next_row() {
    if (!cass_iterator_next(iterator)) return false;

    row = cass_iterator_get_row(iterator);
    return true;
}

const CassValue* requestor::get_column(std::string name) {
    return cass_row_get_column_by_name(row, name.c_str());
}

requestor::~requestor() {
    cass_iterator_free(iterator);
    cass_result_free(result);
    cass_future_free(result_future);
    cass_statement_free(statement);
}