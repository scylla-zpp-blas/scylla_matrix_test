#pragma once

#include <cstddef>
#include <cstdint>
#include <string>

#include "cassandra.h"

#include "tuple_binder.hh"
#include "../scd_exceptions.hh"

namespace {
    // TODO: append null, bool, bytes, custom, uuid, inet, decimal, duration, user_type
    // TODO: handle not only std::collection, but everything that's interface-compatibile with collection
    template<typename T>
    struct collection_appender_helper {
        static void append_to_collection(CassCollection* collection, T value) {
            static_assert(sizeof(T) != sizeof(T), "Binding not implementend for this type");
        };
    };

    template<>
    struct collection_appender_helper<int8_t>{
        static void append_to_collection(CassCollection* collection, int8_t value) {
            throw_on_cass_error(cass_collection_append_int8(collection, value));
        }
    };


    template<>
    struct collection_appender_helper<int16_t> {
        static void append_to_collection(CassCollection* collection, int16_t value) {
            throw_on_cass_error(cass_collection_append_int16(collection, value));
        }
    };


    template<>
    struct collection_appender_helper<int32_t> {
        static void append_to_collection(CassCollection* collection, int32_t value) {
            throw_on_cass_error(cass_collection_append_int32(collection, value));
        }
    };


    template<>
    struct collection_appender_helper<uint32_t> {
        static void append_to_collection(CassCollection* collection, uint32_t value) {
            throw_on_cass_error(cass_collection_append_uint32(collection, value));
        }
    };


    template<>
    struct collection_appender_helper<int64_t> {
        static void append_to_collection(CassCollection* collection, int64_t value) {
            throw_on_cass_error(cass_collection_append_int64(collection, value));
        }
    };


    template<>
    struct collection_appender_helper<float> {
        static void append_to_collection(CassCollection* collection, float value) {
            throw_on_cass_error(cass_collection_append_float(collection, value));
        }
    };

    template<>
    struct collection_appender_helper<double> {
        static void append_to_collection(CassCollection* collection, double value) {
            throw_on_cass_error(cass_collection_append_double(collection, value));
        }
    };


    template<>
    struct collection_appender_helper<const std::string&> {
        static void append_to_collection(CassCollection* collection, const std::string& value) {
            throw_on_cass_error(cass_collection_append_string(collection, value.c_str()));
        }
    };


    template<typename... Types>
    struct collection_appender_helper<std::tuple<Types...>> {
        static void append_to_collection(CassCollection* collection, const std::tuple<Types...>& value) {
            CassTuple *t = cass_tuple_from_tuple(value);
            auto cleanup = [=]() { cass_tuple_free(t); };
            throw_on_cass_error(cass_collection_append_tuple(collection, t), cleanup);
            cleanup();
        }
    };

    template<>
    struct collection_appender_helper<CassCollection *> {
        static void append_to_collection(CassCollection* collection, CassCollection * value) {
            throw_on_cass_error(cass_collection_append_collection(collection, value));
        }
    };
}


template<typename T>
void append_to_collection(CassCollection* collection, T value) {
    ::collection_appender_helper<T>::append_to_collection(collection, value);
}