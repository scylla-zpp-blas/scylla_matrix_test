#pragma once

#include <string>
#include <memory>
#include <stdexcept>
#include <utility>

#include "../../utils/connector.hh"
#include "value_converters/statement_binder.hh"


class scd_statement {
    CassStatement *_statement;
    int _bind_idx = 0;
public:
    explicit scd_statement(CassStatement *statement);
    scd_statement(const std::string& query, size_t arg_count);
    explicit scd_statement(const std::string& query);

    template<typename... Types>
    scd_statement& bind(Types... args)  {
        (bind_to_statement(_statement, _bind_idx++, args), ...);
        return *this;
    }

    void reset(size_t arg_count);

    // We can't really copy this class
    scd_statement(const scd_statement& other) = delete;
    scd_statement& operator=(const scd_statement& other) = delete;

    scd_statement(scd_statement&& other) noexcept;
    scd_statement& operator=(scd_statement&& other) noexcept;

    [[nodiscard]] const CassStatement *get_statement()  const;

    ~scd_statement();
};
