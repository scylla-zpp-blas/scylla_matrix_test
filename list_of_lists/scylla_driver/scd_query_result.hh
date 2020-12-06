#pragma once

#include <string>

#include "cassandra.h"
#include "value_converters/value_retriever.hh"


class scd_query_result {
    const CassResult* _result;
    CassIterator* _iterator;
    const CassRow* _row;
public:
    explicit scd_query_result(const CassResult* res);

    // We can't really copy this class
    scd_query_result(const scd_query_result& other) = delete;
    scd_query_result& operator=(const scd_query_result& other) = delete;

    scd_query_result(scd_query_result&& other) noexcept;
    scd_query_result& operator=(scd_query_result&& other) noexcept;

    /* Prepares the next row of the request response.
     * Returns true if successful, false if there are no rows left to process.
     */
    bool next_row();

    /* Get the CassValue for given column in the currently processed row. */
    template<typename T>
    T get_column(const std::string& name) {
        const CassValue *cell = cass_row_get_column_by_name(_row, name.c_str());
        return retrieve_value<T>(cell);
    }

    template<typename T>
    T get_column(size_t index) {
        const CassValue *cell = cass_row_get_column(_row, index);
        return retrieve_value<T>(cell);
    }

    const CassValue *get_column_raw(const std::string& name);

    const CassValue *get_column_raw(size_t index);

    bool is_column_null(const std::string& name);
    bool is_column_null(size_t index);

    std::string get_column_name(size_t index);

    ~scd_query_result();
};
