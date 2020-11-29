#ifndef SCYLLA_MATRIX_TEST_QUERY_RESULT_HH
#define SCYLLA_MATRIX_TEST_QUERY_RESULT_HH

#include "cassandra.h"
#include <string>

class query_result {
    const CassResult* _result;
    CassIterator* _iterator;
    const CassRow* _row;
public:
    query_result(const CassResult* res);

    /* Prepares the next row of the request response.
     * Returns true if successful, false if there are no rows left to process.
     */
    bool next_row();

    /* Get the CassValue for given column in the currently processed row. */
    const CassValue* get_column(const std::string& name);

    ~query_result();
};


#endif //SCYLLA_MATRIX_TEST_QUERY_RESULT_HH
