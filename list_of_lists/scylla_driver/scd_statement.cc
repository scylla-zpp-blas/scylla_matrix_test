#include "scd_statement.hh"

scd_statement::scd_statement(CassStatement *statement): _statement(statement) {
}

scd_statement::scd_statement(const std::string &query, size_t arg_count) {
    _statement = cass_statement_new(query.c_str(), arg_count);
    //TODO: set_consistency method somwhere else
    cass_statement_set_consistency(_statement, CASS_CONSISTENCY_QUORUM);
}

scd_statement::~scd_statement() {
    cass_statement_free(_statement);
}

const CassStatement *scd_statement::get_statement() const {
    return _statement;
}

scd_statement::scd_statement(scd_statement &&other) noexcept :
    _statement(std::exchange(other._statement, nullptr)),
    _bind_idx(std::exchange(other._bind_idx, 0)) {
}

scd_statement &scd_statement::operator=(scd_statement &&other) noexcept {
    std::swap(_statement, other._statement);
    std::swap(_bind_idx, other._bind_idx);
    return *this;
}

scd_statement::scd_statement(const std::string &query) : scd_statement(query, 0) {}

void scd_statement::reset(size_t arg_count) {
    throw_on_cass_error(cass_statement_reset_parameters(_statement, arg_count));
}
