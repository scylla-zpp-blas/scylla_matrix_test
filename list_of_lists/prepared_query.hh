//
// Created by hayven on 21.11.2020.
//

#ifndef SCYLLA_MATRIX_TEST_REQUESTOR_HH
#define SCYLLA_MATRIX_TEST_REQUESTOR_HH

#include "../utils/connector.hh"
#include "query_result.hh"
#include <string>
#include <memory>
#include <stdexcept>

namespace {
    template<typename T>
    CassError bind_to_statement(CassStatement* statement, size_t index, T value);
}

template<typename... Types>
class prepared_query {
    const CassPrepared* _prepared;
    std::shared_ptr<connector> _connection;

public:
    prepared_query(const std::shared_ptr<connector>& conn, const char *statement): _connection(conn) {
        using namespace std::string_literals;

        /* Code mostly from http://datastax.github.io/cpp-driver/topics/basics/prepared_statements/ */
        CassFuture* prepare_future
                = cass_session_prepare(conn->get_session(), statement);

        CassError rc = cass_future_error_code(prepare_future);

        if (rc != CASS_OK) {
            cass_future_free(prepare_future);
            throw std::runtime_error("Error while preparing query: "s + cass_error_desc(rc));
        }

        _prepared = cass_future_get_prepared(prepare_future);

        cass_future_free(prepare_future);
    }

    std::unique_ptr<query_result> exec(Types... args) {
        CassStatement* statement = cass_prepared_bind(_prepared);
        cass_statement_set_consistency(statement, CASS_CONSISTENCY_QUORUM);

        // C++17 folding expression
        (bind_to_statement(statement, args), ...);

        CassFuture* result_future = cass_session_execute(_connection->get_session(), statement);
        cass_statement_free(statement);

        if (cass_future_error_code(result_future) == CASS_OK) {
            const CassResult *result = cass_future_get_result(result_future);
            return std::make_unique<query_result>(result);
        } else {
            throw std::runtime_error("Query error");
        }
    }


    ~prepared_query() {
        cass_prepared_free(_prepared);
    }
private:


};

#endif //SCYLLA_MATRIX_TEST_REQUESTOR_HH
