//
// Created by hayven on 21.11.2020.
//

#ifndef SCYLLA_MATRIX_TEST_CONNECTOR_H
#define SCYLLA_MATRIX_TEST_CONNECTOR_H

#include <cassandra.h>

/* A class providing an RAII abstraction for Cassandra/Scylla connections. */
class connector {
    CassCluster* cluster;
    CassSession* session;
    CassFuture* connect_future;

public:
    /* Create a connection with given address and port */
    connector(char* address = nullptr, char* port = nullptr);

    /* Utility function for obtaining session object used in procedures
     * pertaining to the connection.
     */
    CassSession* const get_session();

    ~connector();
};

#endif //SCYLLA_MATRIX_TEST_CONNECTOR_H
