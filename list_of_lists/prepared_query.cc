//
// Created by lorak on 29.11.2020.
//

#include "prepared_query.hh"



namespace {
    // TODO: bind null, bool, bytes, custom, uuid, inet, decimal, duration, user_type

    template<>
    CassError bind_to_statement<int8_t>(CassStatement* statement, size_t index, int8_t value) {
        CassError rc = cass_statement_bind_int8(statement, index, value);
        return rc;
    }

    template<>
    CassError bind_to_statement<int16_t>(CassStatement* statement, size_t index, int16_t value) {
        CassError rc = cass_statement_bind_int16(statement, index, value);
        return rc;
    }

    template<>
    CassError bind_to_statement<int32_t>(CassStatement* statement, size_t index, int32_t value) {
        CassError rc = cass_statement_bind_int32(statement, index, value);
        return rc;
    }

    template<>
    CassError bind_to_statement<uint32_t>(CassStatement* statement, size_t index, uint32_t value) {
        CassError rc = cass_statement_bind_uint32(statement, index, value);
        return rc;
    }

    template<>
    CassError bind_to_statement<int64_t>(CassStatement* statement, size_t index, int64_t value) {
        CassError rc = cass_statement_bind_int64(statement, index, value);
        return rc;
    }

    template<>
    CassError bind_to_statement<float>(CassStatement* statement, size_t index, float value) {
        CassError rc = cass_statement_bind_float(statement, index, value);
        return rc;
    }

    template<>
    CassError bind_to_statement<double>(CassStatement* statement, size_t index, double value) {
        CassError rc = cass_statement_bind_double(statement, index, value);
        return rc;
    }

    template<>
    CassError bind_to_statement<const char *>(CassStatement* statement, size_t index, const char* value) {
        CassError rc = cass_statement_bind_string(statement, index, value);
        return rc;
    }

    template<>
    CassError bind_to_statement<const CassCollection *>(CassStatement* statement, size_t index, const CassCollection * value) {
        CassError rc = cass_statement_bind_collection(statement, index, value);
        return rc;
    }

    template<>
    CassError bind_to_statement<const CassTuple *>(CassStatement* statement, size_t index, const CassTuple * value) {
        CassError rc = cass_statement_bind_tuple(statement, index, value);
        return rc;
    }
}
