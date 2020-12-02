#include "scd_prepared_query.hh"



scd_prepared_query::scd_prepared_query(const CassPrepared *prepared): _prepared(prepared) {}

scd_statement scd_prepared_query::get_statement() {
    CassStatement* statement = cass_prepared_bind(_prepared);
    cass_statement_set_consistency(statement, CASS_CONSISTENCY_QUORUM);

    return scd_statement(statement);
}

scd_prepared_query::scd_prepared_query(scd_prepared_query &&other) noexcept :
    _prepared(std::exchange(other._prepared, nullptr)) {}

scd_prepared_query &scd_prepared_query::operator=(scd_prepared_query &&other) noexcept {
    std::swap(_prepared, other._prepared);
    return *this;
}
