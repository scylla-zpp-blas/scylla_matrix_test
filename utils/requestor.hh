//
// Created by hayven on 21.11.2020.
//

#ifndef SCYLLA_MATRIX_TEST_REQUESTOR_HH
#define SCYLLA_MATRIX_TEST_REQUESTOR_HH

#include "connector.hh"
#include <iostream>
#include <memory>
#include <sstream>

/* A class providing an additional layer of abstraction
 * for Scylla's C++ driver's query/response mechanism
 * to ease the process of writing queries.
 * Implements the RAII idiom to reduce boilerplate in newly written code.
 */
class requestor {
    std::stringstream _query;
    std::shared_ptr<connector> _conn;
    CassStatement* _statement;
    CassFuture* _result_future;
    const CassResult* _result;
    CassIterator* _iterator;
    const CassRow* _row;

public:
    /* Creates a new requestor using connection represented by a given connector */
    requestor(std::shared_ptr<connector> conn);

    /* Appends new elements to the query string */
    template<typename T>
    requestor& operator<<(T t) {
        _query << t;
        return *this;
    }

    /* Writes out the query text */
    friend std::ostream& operator<<(std::ostream& os, requestor& r);

    /* Send the entire query string after it's been composed.
     * Returns true if the request was successful, otherwise false.
     * The subsequent operations can only be used in the first case.
     */
    void send();

    /* Prepares the next row of the request response.
     * Returns true if successful, false if there are no rows left to process.
     */
    bool next_row();

    /* Get the CassValue for given column in the currently processed row. */
    const CassValue* get_column(std::string name);

    ~requestor();
};

#endif //SCYLLA_MATRIX_TEST_REQUESTOR_HH
