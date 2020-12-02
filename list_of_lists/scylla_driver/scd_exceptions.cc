#include "scd_exceptions.hh"
#include "fmt/format.h"


scd_exception::scd_exception(CassError rc) :
    runtime_error(fmt::format("scylla driver exception: {}. Error code: {}", cass_error_desc(rc), rc)),
    _rc(rc) {}

CassError scd_exception::get_error() {
    return _rc;
}

void throw_on_cass_error(CassError rc) {
    if (rc != CASS_OK) {
        throw scd_exception(rc);
    }
}
