//
// Created by hayven on 21.11.2020.
//

#include "connector.hh"
#include <stdexcept>
#include <string>

connector::connector(const char* address, const char* port) {
    _cluster = cass_cluster_new();
    _session = cass_session_new();

    if (address != nullptr) {
        cass_cluster_set_contact_points(_cluster, address);
    }
    if (port != nullptr) {
        cass_cluster_set_port(_cluster, std::stoi(port));
    }

    cass_cluster_set_protocol_version(_cluster, CASS_PROTOCOL_VERSION_V4);
    _connect_future = cass_session_connect(_session, _cluster);

    if (cass_future_error_code(_connect_future) != CASS_OK) {
        throw std::runtime_error("Connection error");
    }
}

CassSession* const connector::get_session() {
    return _session;
}

connector::~connector() {
    cass_future_free(_connect_future);
    cass_cluster_free(_cluster);
    cass_session_free(_session);
}