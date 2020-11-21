//
// Created by hayven on 21.11.2020.
//

#include "connector.h"
#include <stdexcept>
#include <string>

connector::connector(char* address, char* port) {
    cluster = cass_cluster_new();
    session = cass_session_new();

    if (address != nullptr) {
        cass_cluster_set_contact_points(cluster, address);
    }
    if (port != nullptr) {
        cass_cluster_set_port(cluster, std::stoi(port));
    }

    cass_cluster_set_protocol_version(cluster, CASS_PROTOCOL_VERSION_V4);
    connect_future = cass_session_connect(session, cluster);

    if (cass_future_error_code(connect_future) != CASS_OK) {
        throw std::runtime_error("Connection error");
    }
}

CassSession* const connector::get_session() {
    return session;
}

connector::~connector() {
    cass_future_free(connect_future);
    cass_cluster_free(cluster);
    cass_session_free(session);
}