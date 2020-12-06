#pragma once

#include <cstddef>
#include <cstdint>
#include <string>
#include <set>
#include <vector>

#include "cassandra.h"
#include "fmt/format.h"

#include "collection_appender.hh"
#include "../scd_exceptions.hh"

namespace {
    // TODO: bind null, bool, bytes, custom, uuid, inet, decimal, duration, user_type
    template<typename T>
    struct statement_binder_helper {
        static void bind_to_statement(CassStatement* statement, size_t index, T value) {
            static_assert(sizeof(T) != sizeof(T), "Binding not implementend for this type");
        };
    };

    template<>
    struct statement_binder_helper<int8_t>{
        static void bind_to_statement(CassStatement* statement, size_t index, int8_t value) {
            throw_on_cass_error(cass_statement_bind_int8(statement, index, value));
        }
    };


    template<>
    struct statement_binder_helper<int16_t> {
        static void bind_to_statement(CassStatement* statement, size_t index, int16_t value) {
            throw_on_cass_error(cass_statement_bind_int16(statement, index, value));
        }
    };


    template<>
    struct statement_binder_helper<int32_t> {
        static void bind_to_statement(CassStatement* statement, size_t index, int32_t value) {
            throw_on_cass_error(cass_statement_bind_int32(statement, index, value));
        }
    };


    template<>
    struct statement_binder_helper<uint32_t> {
        static void bind_to_statement(CassStatement* statement, size_t index, uint32_t value) {
            throw_on_cass_error(cass_statement_bind_uint32(statement, index, value));
        }
    };


    template<>
    struct statement_binder_helper<int64_t> {
        static void bind_to_statement(CassStatement* statement, size_t index, int64_t value) {
            throw_on_cass_error(cass_statement_bind_int64(statement, index, value));
        }
    };


    template<>
    struct statement_binder_helper<float> {
        static void bind_to_statement(CassStatement* statement, size_t index, float value) {
            throw_on_cass_error(cass_statement_bind_float(statement, index, value));
        }
    };

    template<>
    struct statement_binder_helper<double> {
        static void bind_to_statement(CassStatement* statement, size_t index, double value) {
            throw_on_cass_error(cass_statement_bind_double(statement, index, value));
        }
    };


    template<>
    struct statement_binder_helper<const std::string&> {
        static void bind_to_statement(CassStatement* statement, size_t index, const std::string& value) {
            throw_on_cass_error(cass_statement_bind_string(statement, index, value.c_str()));
        }
    };


    template<typename... Types>
    struct statement_binder_helper<std::tuple<Types...>> {
        static void bind_to_statement(CassStatement* statement, size_t index, const std::tuple<Types...>& value) {
            CassTuple *t = cass_tuple_from_tuple(value);
            auto cleanup = [=]() { cass_tuple_free(t); };
            throw_on_cass_error(cass_statement_bind_tuple(statement, index, t), cleanup);
            cleanup();
        }
    };

    template<typename Type>
    struct statement_binder_helper<std::set<Type>> {
        static void bind_to_statement(CassStatement* statement, size_t index, const std::set<Type>& value) {
            CassCollection *set = cass_collection_new(CASS_COLLECTION_TYPE_SET, value.size());
            auto cleanup = [=]() { cass_collection_free(set); };
            for(const auto& it : value) {
                try {
                    append_to_collection(set, it);
                } catch(const scd_exception& e) {
                    cleanup();
                    throw e;
                }
            }
            cass_statement_bind_collection(statement, index, set);
            cleanup();
        }
    };

    template<typename Type>
    struct statement_binder_helper<std::vector<Type>> {
        static void bind_to_statement(CassStatement* statement, size_t index, const std::vector<Type>& value) {
            CassCollection *set = cass_collection_new(CASS_COLLECTION_TYPE_SET, value.size());
            auto cleanup = [=]() { cass_collection_free(set); };
            for(const auto& it : value) {
                try {
                    append_to_collection(set, it);
                } catch(const scd_exception& e) {
                    cleanup();
                    throw e;
                }
            }
            cass_statement_bind_collection(statement, index, set);
            cleanup();
        }
    };

    template<>
    struct statement_binder_helper<CassCollection *> {
        static void bind_to_statement(CassStatement* statement, size_t index, CassCollection * value) {
            throw_on_cass_error(cass_statement_bind_collection(statement, index, value));
        }
    };
}


template<typename T>
void bind_to_statement(CassStatement* statement, size_t index, T value) {
    ::statement_binder_helper<T>::bind_to_statement(statement, index, value);
}