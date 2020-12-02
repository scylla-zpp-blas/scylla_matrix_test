#pragma once

#include <string>
#include <stdexcept>

#include "cassandra.h"

class scd_exception : public std::runtime_error {
    CassError _rc;
public:
    scd_exception(CassError rc);

    CassError get_error();
};

void throw_on_cass_error(CassError rc);

template<typename Func>
void throw_on_cass_error(CassError rc, Func cleanup_func) {
    if (rc != CASS_OK) {
        cleanup_func();
        throw scd_exception(rc);
    }
}

