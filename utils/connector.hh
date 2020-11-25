//
// Created by hayven on 21.11.2020.
//

#ifndef SCYLLA_MATRIX_TEST_CONNECTOR_HH
#define SCYLLA_MATRIX_TEST_CONNECTOR_HH

#include <cassandra.h>

/* A class providing an RAII abstraction for Cassandra/Scylla connections. */
class connector {
    CassCluster* _cluster;
    CassSession* _session;
    CassFuture* _connect_future;

public:
    /* Create a connection with given address and port */
    connector(const char* address = nullptr, const char* port = nullptr);

    /* Utility function for obtaining session object used in procedures
     * pertaining to the connection.
     */
    CassSession* const get_session();

    ~connector();
};

#endif //SCYLLA_MATRIX_TEST_CONNECTOR_HH
