#include <map>
#include <memory>
#include <string>
#include <utility>
#include <set>
#include <vector>

#include <cassandra.h>
#include "fmt/format.h"

#include "../float_value_factory.hh"
#include "../sparse_matrix_value_generator.hh"
#include "scylla_driver/prepared_query.hh"
#include "scylla_driver/session.hh"


namespace {
    constexpr size_t columns_in_row = 10;


    const std::string namespace_name = "zpp";
    const std::string table_name_rows = "lil_rows";
    const std::string table_name_columns = "lil_columns";
    const std::string table_name_meta = "lil_meta";

    /* ================ TABLE CREATION ============== */
    const std::string create_keyspace_query = fmt::format(R"(
CREATE KEYSPACE IF NOT EXISTS {0} WITH REPLICATION = {{
    'class' : 'SimpleStrategy',
    'replication_factor' : 1
}};
)", namespace_name);

//TODO: better primary key
    const std::string create_rows_table_query = fmt::format(R"(
CREATE TABLE {0}.{1} (
    matrix_id int,
    row bigint,
    part bigint,
    filled int,
    {{}},
    PRIMARY KEY (matrix_id, row, part)
) WITH CLUSTERING ORDER BY (row ASC, part ASC);
)", namespace_name, table_name_rows);

    const std::string create_columns_table_query = fmt::format(R"(
CREATE TABLE {0}.{1} (
    matrix_id int,
    column bigint,
    part bigint,
    filled int,
    {{}},
    PRIMARY KEY (matrix_id, column, part)
) WITH CLUSTERING ORDER BY (column ASC, part ASC);
)", namespace_name, table_name_columns);

    const std::string create_meta_table_query = fmt::format(R"(
CREATE TABLE {0}.{1} (
    matrix_id int,
    height bigint,
    width bigint,
    PRIMARY KEY (matrix_id, height)
);
)", namespace_name, table_name_meta);

    /* ============= END TABLE CREATION ========== */

    /* ============ META QUERIES ============= */
    const std::string list_matrices_query = fmt::format(R"(
SELECT * FROM {0}.{1};
)", namespace_name, table_name_meta);

    const std::string get_max_matrix_id_query = fmt::format(R"(
SELECT MAX(matrix_id) AS id FROM {0}.{1};
)", namespace_name, table_name_meta);

    const std::string new_matrix_meta_query = fmt::format(R"(
INSERT INTO {0}.{1} (matrix_id, height, width) VALUES (?, ?, ?);
)", namespace_name, table_name_meta);
    /* =========== END META QUERIES =========== */

    /* =========== CREATING MATRIX ============ */
    const std::string matrix_insert_row_query = fmt::format(R"(
INSERT INTO {0}.{1} (matrix_id, row, part, filled, {{}}) VALUES (?, ?, ?, ?, {{}});
)", namespace_name, table_name_rows);

    const std::string matrix_init_column_part = fmt::format(R"(
INSERT INTO {0}.{1} (matrix_id, column, part, filled) VALUES (?, ?, ?, 0);
)", namespace_name, table_name_columns);

    const std::string matrix_append_to_column = fmt::format(R"(
UPDATE {0}.{1} SET filled = ?, {{}} = ?, {{}} = ? WHERE matrix_id = ? AND column = ? AND part = ?;
)", namespace_name, table_name_columns);

    /* =========== END CREATING MATRIX ============ */

    const std::string matrix_fetch_row_list = fmt::format(R"(
SELECT row FROM {0}.{1} WHERE matrix_id = ?;
)", namespace_name, table_name_rows);

    const std::string matrix_fetch_column_list = fmt::format(R"(
SELECT column FROM {0}.{1} WHERE matrix_id = ?;
)", namespace_name, table_name_columns);

    const std::string matrix_fetch_row_part = fmt::format(R"(
SELECT * FROM {0}.{1} WHERE matrix_id = ? AND row = ? AND part = ?;
)", namespace_name, table_name_rows);

    const std::string matrix_fetch_column_part = fmt::format(R"(
SELECT * FROM {0}.{1} WHERE matrix_id = ? AND column = ? AND part = ?;
)", namespace_name, table_name_columns);

    const std::string matrix_fetch_whole_row = fmt::format(R"(
SELECT * FROM {0}.{1} WHERE matrix_id = ? AND row = ?;
)", namespace_name, table_name_rows);

    const std::string matrix_fetch_whole_column = fmt::format(R"(
SELECT * FROM {0}.{1} WHERE matrix_id = ? AND column = ?;
)", namespace_name, table_name_columns);

    const std::string fetch_whole_matrix_query = R"(
SELECT * FROM {}.{} WHERE matrix_id = ?;
)";

    const std::string fetch_matrix_info_query = fmt::format(R"(
SELECT * FROM {}.{} WHERE matrix_id = ?;
)", namespace_name, table_name_meta);

    const std::string delete_matrix_query = R"(
DELETE FROM {}.{} WHERE matrix_id = ?;
)";

    const std::string drop_table_query = R"(
DROP TABLE IF EXISTS {}.{};
)";
}

template<typename T>
class LIL {
    using row_data_t = std::vector<std::tuple<int64_t, T>>;
    std::shared_ptr<scmd::session> _sess;
    std::vector<std::string> _data_columns;

public:
    explicit LIL(std::shared_ptr<scmd::session> conn) :
            _sess(std::move(conn)) {
        /* Make sure that the necessary namespaces and table exist */
        _sess->execute(create_keyspace_query);


        for(size_t i = 0; i < columns_in_row; i++) {
            _data_columns.push_back(fmt::format("i_{}", i));
            _data_columns.push_back(fmt::format("v_{}", i));
        }
    }

    void create_tables() {
        std::vector<std::string> _data_columns_with_types;
        for(size_t i = 0; i < columns_in_row; i++) {
            _data_columns_with_types.push_back(fmt::format("i_{} bigint", i));
            _data_columns_with_types.push_back(fmt::format("v_{} double", i));
        }
        /* ========== DROP TABLES ======= */
        _sess->execute(fmt::format(drop_table_query, namespace_name, table_name_rows));
        _sess->execute(fmt::format(drop_table_query, namespace_name, table_name_columns));
        _sess->execute(fmt::format(drop_table_query, namespace_name, table_name_meta));

        /* =========== CREATE TABLES ========== */
        std::string rows_query = fmt::format(create_rows_table_query, fmt::join(_data_columns_with_types, ", "));
        //fmt::print(rows_query);
        _sess->execute(rows_query);
        std::string cols_query = fmt::format(create_columns_table_query, fmt::join(_data_columns_with_types, ", "));
        //fmt::print(cols_query);
        _sess->execute(cols_query);
        _sess->execute(create_meta_table_query);
    }

    std::vector<std::tuple<int32_t, int64_t, int64_t>> fetch_list() {
        std::vector<std::tuple<int32_t, int64_t, int64_t>> result;

        auto list_result = _sess->execute(list_matrices_query);

        while(list_result.next_row()) {
            auto id = list_result.get_column<int32_t>("matrix_id");
            auto width = list_result.get_column<int64_t>("width");
            auto height = list_result.get_column<int64_t>("height");
            result.emplace_back(id, width, height);
        }

        return result;
    }

    // Loads matrix from generator to database. Return matrix id.
    size_t load_matrix(matrix_value_generator<T>&& gen) {
        int32_t new_id = register_new_matrix(gen.height(), gen.width());
        std::set<int64_t> columns = generate_row_matrix(new_id, std::move(gen));
        create_column_matrix(new_id, columns);
        return new_id;
    }

    void delete_matrix(int32_t id) {
        _sess->execute(scmd::statement(fmt::format(delete_matrix_query, namespace_name, table_name_rows), 1).bind(id));
        _sess->execute(scmd::statement(fmt::format(delete_matrix_query, namespace_name, table_name_columns), 1).bind(id));
        _sess->execute(scmd::statement(fmt::format(delete_matrix_query, namespace_name, table_name_meta), 1).bind(id));
    }

    /* Multiplies two matrices loaded into Scylla with load_matrix. Returns index of new matrix */
    size_t multiply(size_t a, size_t b) {
        const auto& [a_height, a_width] = get_dimensions(a);
        const auto& [b_height, b_width] = get_dimensions(b);
        if(a_width != b_height) {
            throw std::runtime_error(fmt::format("Invalid matrix dimensions, cant multiply ({} x {}), ({}, {})", a_height, a_width, b_height, b_width));
        }

        scmd::prepared_query insert_row_prepared = get_row_inserter();

        auto a_rows = get_row_list(a);
        auto b_columns = get_column_list(b);

        int32_t c = register_new_matrix(a_height, b_width);

        std::set<int64_t> columns;
        row_data_t c_row_data;
        int64_t part = 0;

        for(auto a_row_idx : a_rows) {
            row_data_t a_row = get_row(a, a_row_idx);
            for(auto b_col_idx : b_columns) {
                columns.emplace(b_col_idx);
                row_data_t b_col = get_column(b, b_col_idx);
                T value = multiply_single_cell(a_row, b_col);
                if(value != 0.0) { // TODO: probably wrong way to do it
                    if(c_row_data.size() == columns_in_row) {
                        submit_row_data(insert_row_prepared, c, a_row_idx, part, c_row_data);
                        c_row_data.clear();
                        part++;
                    }
                    c_row_data.emplace_back(b_col_idx, value);
                }
            }

            if(!c_row_data.empty()) {
                submit_row_data(insert_row_prepared, c, a_row_idx, part, c_row_data);
                c_row_data.clear();
            }
            part = 0;
        }

        create_column_matrix(c, columns);

        return c;
    }

    std::vector<std::vector<T>> get_matrix_as_dense(int32_t matrix_id) {
        const auto& [height, width] = get_dimensions(matrix_id);

        std::vector<std::vector<T>> result(height, std::vector<T>(width, 0.0));
        auto query_result = _sess->execute(scmd::statement(fmt::format(fetch_whole_matrix_query, namespace_name, table_name_rows), 1).bind(matrix_id));

        while(query_result.next_row()) {
            auto r_row = query_result.get_column<int64_t>("row");
            auto filled = query_result.get_column<int32_t>("filled");
            for(int i = 0; i < filled; i++) {
                auto r_column = query_result.get_column<int64_t>(_data_columns[2 * i]);
                T r_value = query_result.get_column<double>(_data_columns[2 * i + 1]);
                result[r_row - 1][r_column - 1] = r_value;
            }
        }

        return result;
    }


private:
    T multiply_single_cell(const row_data_t& row, const row_data_t& column) {
        T result = 0.0;

        int32_t i = 0, j = 0;
        while(i < row.size() && j < column.size()) {
            const auto &[row_i, row_v] = row[i];
            const auto &[col_i, col_v] = column[j];

            if(row_i == col_i) {
                result += row_v * col_v;
                i++;
                j++;
            } else if(row_i < col_i) {
                i++;
            } else {
                j++;
            }
        }

        return result;
    }

    row_data_t db_row_to_row_data(scmd::query_result& result) {
        row_data_t ret;
        while(result.next_row()) {
            auto filled = result.get_column<int32_t>("filled");
            for(int32_t i = 0; i < filled; i++) {
                // TODO: make sure column order is correct (in case it somehow changes in the future)
                auto idx = result.get_column<int64_t>(4 + i);
                T value = result.get_column<double>(4 + columns_in_row + i);
                ret.emplace_back(idx, value);
            }
        }

        return ret;
    }

    row_data_t get_row(int32_t matrix_id, int64_t row) {
        auto result = _sess->execute(scmd::statement(matrix_fetch_whole_row, 2).bind(matrix_id, row));
        return db_row_to_row_data(result);
    }

    row_data_t get_column(int32_t matrix_id, int64_t column) {
        auto result = _sess->execute(scmd::statement(matrix_fetch_whole_column, 2).bind(matrix_id, column));
        return db_row_to_row_data(result);
    }

    int32_t register_new_matrix(int64_t height, int64_t width) {
        int32_t new_id = get_new_matrix_id();
        _sess->execute(scmd::statement(new_matrix_meta_query, 3).bind(new_id, height, width));
        return new_id;
    }


    std::pair<int64_t, int64_t> get_dimensions(int32_t id) {
        auto result = _sess->execute(scmd::statement(fetch_matrix_info_query, 1).bind(id));
        if(!result.next_row()) {
            return {0, 0};
        }
        return std::make_pair(result.get_column<int64_t>("height"), result.get_column<int64_t>("width"));
    }


    int32_t get_new_matrix_id() {
        auto res = _sess->execute(get_max_matrix_id_query);
        if(!res.next_row() || res.is_column_null("id")) return 0;
        return res.get_column<int32_t>("id") + 1;
    }

    std::set<int64_t> get_row_list(int32_t matrix_id) {
        auto query_result = _sess->execute(scmd::statement(matrix_fetch_row_list, 1).bind(matrix_id));
        std::set<int64_t> rows;
        while(query_result.next_row()) {
            auto row_num = query_result.get_column<int64_t>("row");
            rows.insert(row_num);
        }

        return rows;
    }

    std::set<int64_t> get_column_list(int32_t matrix_id) {
        auto query_result = _sess->execute(scmd::statement(matrix_fetch_column_list, 1).bind(matrix_id));
        std::set<int64_t> columns;
        while(query_result.next_row()) {
            auto col_num = query_result.get_column<int64_t>("column");
            columns.insert(col_num);
        }

        return columns;
    }

    scmd::prepared_query get_row_inserter() {
        scmd::prepared_query insert_row_prepared = _sess->prepare(
                fmt::format(matrix_insert_row_query,
                            fmt::join(_data_columns, ", "),
                            fmt::join(std::vector<char>(columns_in_row * 2, '?'), ", ")));
        return insert_row_prepared;
    }

    void submit_row_data(scmd::prepared_query& prepared_query, int32_t matrix_id, int64_t row, int64_t part, const row_data_t& row_data) {
        auto stmt = prepared_query.get_statement();
        stmt.bind(matrix_id, row, part, (int32_t)row_data.size());
        for(auto entry : row_data) {
            stmt.bind(std::get<0>(entry), (double)std::get<1>(entry));
        }
        _sess->execute(stmt);
    }

    std::set<int64_t> generate_row_matrix(int32_t id, matrix_value_generator<T>&& gen) {
        scmd::prepared_query insert_row_prepared = get_row_inserter();

        std::set<int64_t> columns;
        row_data_t row_data;
        int64_t row = 1;
        int64_t part = 0;

        while (gen.has_next()) {
            matrix_value<T> next = gen.next();
            columns.emplace(next.j);
            //fmt::print(stderr, "Next cell: [{}, {}] : {}\n", next.i, next.j, next.val);
            if(next.i != row || row_data.size() == columns_in_row) {
                //fmt::print(stderr, "row: {}. part: {}, size: {}, next.i: {}\n", row, part, row_data.size(), next.i);
                submit_row_data(insert_row_prepared, id, row, part, row_data);
                row_data.clear();
                part = (next.i == row) ? part + 1 : 0;
                row = next.i;
            }
            row_data.emplace_back(next.j, next.val);

        }

        if(!row_data.empty()) {
            submit_row_data(insert_row_prepared, id, row, part, row_data);
            row_data.clear();
        }

        return columns;
    }

    void create_column_matrix(int32_t id, const std::set<int64_t>& columns) {
        scmd::prepared_query _fetch_row_part_prepared  = _sess->prepare(matrix_fetch_row_part);
        scmd::prepared_query init_column_prepared = _sess->prepare(matrix_init_column_part);

        // column -> (part, filled)
        std::map<int64_t, std::pair<int64_t, int32_t>> column_info;

        for(int64_t column : columns) {
            column_info.insert({column,  {0, 0}});
            _sess->execute(init_column_prepared.get_statement().bind(id, column, (int64_t)0));
        }

        std::set<int64_t> rows = get_row_list(id);
        for(int64_t rownum : rows) {
            int64_t part = 0;
            do {
                auto result = _sess->execute(_fetch_row_part_prepared.get_statement().bind(id, rownum, part));
                // SELECT * FROM {0}.{1} WHERE matrix_id = ? AND row = ? AND part = ?;
                if (!result.next_row()) {
                    break;
                }
                auto filled = result.get_column<int32_t>("filled");
                for (int i = 0; i < filled; i++) {
                    // TODO: make sure column order is correct (in case it somehow changes in the future)
                    auto column = result.get_column<int64_t>(4 + i);
                    T value = result.get_column<double>(4 + columns_in_row + i);
                    auto info = column_info.find(column);
                    if(info->second.second == columns_in_row) {
                        info->second.first++;
                        info->second.second = 0;
                        _sess->execute(init_column_prepared.get_statement().bind(id, column, info->second.first));
                    }
                    int32_t idx = info->second.second;
                    std::string stmt_str = fmt::format(matrix_append_to_column, _data_columns[2 * idx], _data_columns[2 * idx + 1]);
                    //fmt::print(stmt_str);
                    info->second.second++;
                    _sess->execute(scmd::statement(stmt_str, 6).bind(info->second.second, rownum, (double)value, id, column, info->second.first));
                }
                part++;
            } while (true);
        }
    }
};
