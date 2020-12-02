#pragma once


#pragma once

#include <cstddef>
#include <cstdint>
#include <string>
#include <tuple>

#include "cassandra.h"

#include "../scd_exceptions.hh"

namespace {
    // TODO: bind null, bool, bytes, custom, uuid, inet, decimal, duration, user_type
    // TODO: handle not only std::tuple, but everything that's interface-compatibile with tuple
    template<typename T>
    struct tuple_binder_helper {
        static void bind_to_tuple(CassTuple* tuple, size_t index, T value) {
            static_assert(sizeof(T) != sizeof(T), "Binding not implementend for this type");
        };
    };

    template<>
    struct tuple_binder_helper<int8_t>{
        static void bind_to_tuple(CassTuple* tuple, size_t index, int8_t value) {
            throw_on_cass_error(cass_tuple_set_int8(tuple, index, value));
        }
    };


    template<>
    struct tuple_binder_helper<int16_t> {
        static void bind_to_tuple(CassTuple* tuple, size_t index, int16_t value) {
            throw_on_cass_error(cass_tuple_set_int16(tuple, index, value));
        }
    };


    template<>
    struct tuple_binder_helper<int32_t> {
        static void bind_to_tuple(CassTuple* tuple, size_t index, int32_t value) {
            throw_on_cass_error(cass_tuple_set_int32(tuple, index, value));
        }
    };


    template<>
    struct tuple_binder_helper<uint32_t> {
        static void bind_to_tuple(CassTuple* tuple, size_t index, uint32_t value) {
            throw_on_cass_error(cass_tuple_set_uint32(tuple, index, value));
        }
    };


    template<>
    struct tuple_binder_helper<int64_t> {
        static void bind_to_tuple(CassTuple* tuple, size_t index, int64_t value) {
            throw_on_cass_error(cass_tuple_set_int64(tuple, index, value));
        }
    };


    template<>
    struct tuple_binder_helper<float> {
        static void bind_to_tuple(CassTuple* tuple, size_t index, float value) {
            throw_on_cass_error(cass_tuple_set_float(tuple, index, value));
        }
    };

    template<>
    struct tuple_binder_helper<double> {
        static void bind_to_tuple(CassTuple* tuple, size_t index, double value) {
            throw_on_cass_error(cass_tuple_set_double(tuple, index, value));
        }
    };


    template<>
    struct tuple_binder_helper<const std::string&> {
        static void bind_to_tuple(CassTuple* tuple, size_t index, const std::string& value) {
            throw_on_cass_error(cass_tuple_set_string(tuple, index, value.c_str()));
        }
    };


    template<typename... Types>
    struct tuple_binder_helper<std::tuple<Types...>> {
        static void bind_to_tuple(CassTuple* tuple, size_t index, const std::tuple<Types...>& value) {
            CassTuple *t = cass_tuple_from_tuple(value);
            auto cleanup = [=]() { cass_tuple_free(t); };
            throw_on_cass_error(cass_tuple_set_tuple(tuple, index, t), cleanup);
            cleanup();
        }
    };

    template<>
    struct tuple_binder_helper<CassCollection *> {
        static void bind_to_tuple(CassTuple* tuple, size_t index, CassCollection * value) {
            throw_on_cass_error(cass_tuple_set_collection(tuple, index, value));
        }
    };


    template<std::size_t I = 0, typename... Types>
    inline typename std::enable_if<I == sizeof...(Types), void>::type
    tuple_binder_iterator(CassTuple *t, const std::tuple<Types...>& value)
    { }

    template<std::size_t I = 0, typename... Types>
    inline typename std::enable_if<I < sizeof...(Types), void>::type
    tuple_binder_iterator(CassTuple *t, const std::tuple<Types...>& value)
    {
        bind_to_tuple(t, I, std::get<I>(value));
        tuple_binder_iterator<I + 1, Types...>(t, value);
    }
}

template<typename... Types>
CassTuple *cass_tuple_from_tuple(const std::tuple<Types...>& value) {
    CassTuple *t = cass_tuple_new(sizeof...(Types));
    auto cleanup = [=]() { cass_tuple_free(t); };
    try {
        tuple_binder_iterator(t, value);
    }catch (const scd_exception& e) {
        cleanup();
        throw e;
    }

    return t;
}


template<typename T>
void bind_to_tuple(CassTuple* tuple, size_t index, T value) {
    ::tuple_binder_helper<T>::bind_to_tuple(tuple, index, value);
}