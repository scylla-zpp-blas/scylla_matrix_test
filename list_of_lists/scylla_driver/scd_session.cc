//
// Created by lorak on 01.12.2020.
//

#include "scd_session.hh"
#include "scd_exceptions.hh"

scd_session::scd_session(const std::string &address, const std::string &port) {
    _cluster = cass_cluster_new();
    _session = cass_session_new();

    cass_cluster_set_contact_points(_cluster, address.c_str());

    if (!port.empty()) {
        cass_cluster_set_port(_cluster, std::stoi(port));
    }

    cass_cluster_set_protocol_version(_cluster, CASS_PROTOCOL_VERSION_V4);
    CassFuture* connect_future = cass_session_connect(_session, _cluster);
    auto cleanup = [=]() { cass_future_free(connect_future); };

    throw_on_cass_error(cass_future_error_code(connect_future), cleanup);
    cleanup();
}

scd_session::~scd_session() {
    cass_cluster_free(_cluster);
    cass_session_free(_session);
}

scd_query_result scd_session::execute(const scd_statement& statement) {
    CassFuture* result_future = cass_session_execute(_session, statement.get_statement());
    auto cleanup = [=]() { cass_future_free(result_future); };

    throw_on_cass_error(cass_future_error_code(result_future), cleanup);
    const CassResult *result = cass_future_get_result(result_future);
    cleanup();
    return scd_query_result(result);
}

scd_prepared_query scd_session::prepare(std::string query) {
    using namespace std::string_literals;

    /* Code mostly from http://datastax.github.io/cpp-driver/topics/basics/prepared_statements/ */
    CassFuture* prepare_future
            = cass_session_prepare(_session, query.c_str());

    throw_on_cass_error(cass_future_error_code(prepare_future), [=]() { cass_future_free(prepare_future); });

    const CassPrepared *prepared = cass_future_get_prepared(prepare_future);
    cass_future_free(prepare_future);

    return scd_prepared_query(prepared);
}

scd_session::scd_session(scd_session &&other) noexcept :
    _cluster(std::exchange(other._cluster, nullptr)),
    _session(std::exchange(other._session, nullptr)) {}

scd_session &scd_session::operator=(scd_session &&other) noexcept {
    std::swap(_cluster, other._cluster);
    std::swap(_session, other._session);
    return *this;
}

scd_query_result scd_session::execute(const std::string &query) {
    return execute(scd_statement(query));
}
