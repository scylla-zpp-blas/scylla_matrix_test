#pragma once

#include <string>
#include "cassandra.h"
#include "scd_query_result.hh"
#include "scd_statement.hh"
#include "scd_prepared_query.hh"

//TODO: divide into cluster and session, do this class better
class scd_session {
    CassCluster* _cluster;
    CassSession* _session;

public:
    explicit scd_session(const std::string& address, const std::string& port = "");

    // We can't really copy this class
    scd_session(const scd_session& other) = delete;
    scd_session& operator=(const scd_session& other) = delete;

    scd_session(scd_session&& other) noexcept;
    scd_session& operator=(scd_session&& other) noexcept;

    scd_query_result execute(const scd_statement& statement);
    // Convenience function to avoid creating scd_statement manually for simple queries
    scd_query_result execute(const std::string& query);

    scd_prepared_query prepare(std::string query);

    ~scd_session();
};
