#pragma once

#include <string>
#include <memory>
#include <stdexcept>

#include "../../utils/connector.hh"
#include "scd_query_result.hh"
#include "value_converters/statement_binder.hh"
#include "scd_statement.hh"

class scd_prepared_query {
    const CassPrepared* _prepared;

public:
    explicit scd_prepared_query(const CassPrepared* prepared);

    // We can't really copy this class
    scd_prepared_query(const scd_prepared_query& other) = delete;
    scd_prepared_query& operator=(const scd_prepared_query& other) = delete;

    scd_prepared_query(scd_prepared_query&& other) noexcept;
    scd_prepared_query& operator=(scd_prepared_query&& other) noexcept;

    scd_statement get_statement();

    ~scd_prepared_query() {
        cass_prepared_free(_prepared);
    }
};
