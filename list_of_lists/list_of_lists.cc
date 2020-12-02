#include <map>
#include <memory>
#include <string>
#include <utility>
#include <set>

#include <cassandra.h>
#include <unordered_map>
#include "fmt/format.h"

#include "../float_value_factory.hh"
#include "../sparse_matrix_value_generator.hh"
#include "scylla_driver/scd_prepared_query.hh"
#include "scylla_driver/scd_session.hh"


namespace {
    constexpr size_t columns_in_row = 7;


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
) WITH CLUSTERING ORDER BY (row ASC);
)", namespace_name, table_name_rows);

    const std::string create_columns_table_query = fmt::format(R"(
CREATE TABLE {0}.{1} (
    matrix_id int,
    column bigint,
    part bigint,
    filled int,
    {{}},
    PRIMARY KEY (matrix_id, column, part)
) WITH CLUSTERING ORDER BY (column ASC);
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

    const std::string matrix_fetch_row_part = fmt::format(R"(
SELECT * FROM {0}.{1} WHERE matrix_id = ? AND row = ? AND part = ?;
)", namespace_name, table_name_rows);

    const std::string matrix_fetch_row = fmt::format(R"(
SELECT * FROM {0}.{1} WHERE matrix_id = ? AND row = ?;
)", namespace_name, table_name_rows);

    const std::string fetch_whole_matrix_query = R"(
SELECT * FROM {} WHERE matrix_id = ?;
)";

    const std::string delete_matrix_query = R"(
DELETE FROM {}.{} WHERE matrix_id = ?;
)";

    const std::string drop_table_query = R"(
DROP TABLE IF EXISTS {}.{};
)";
}

template<typename T>
class LIL {
    std::shared_ptr<scd_session> _sess;
    std::vector<std::string> _data_columns;

public:
    explicit LIL(std::shared_ptr<scd_session> conn) :
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
        fmt::print(rows_query);
        _sess->execute(rows_query);
        std::string cols_query = fmt::format(create_columns_table_query, fmt::join(_data_columns_with_types, ", "));
        fmt::print(cols_query);
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
        int32_t new_id = get_new_matrix_id() + 1;
        int64_t height = gen.height();
        int64_t width = gen.width();
        _sess->execute(scd_statement(new_matrix_meta_query, 3).bind(new_id, height, width));
        std::set<int64_t> columns = generate_row_matrix(new_id, std::move(gen));
        create_column_matrix(new_id, columns);

        return new_id;
    }

    void delete_matrix(int64_t id) {
        _sess->execute(scd_statement(fmt::format(delete_matrix_query, namespace_name, table_name_rows), 1).bind(id));
        _sess->execute(scd_statement(fmt::format(delete_matrix_query, namespace_name, table_name_columns), 1).bind(id));
        _sess->execute(scd_statement(fmt::format(delete_matrix_query, namespace_name, table_name_meta), 1).bind(id));
    }

    /* Multiplies two matrices loaded into Scylla with load_matrix. Returns index of new matrix */
    size_t multiply(size_t a, size_t b) {
        return 0;
    }

    std::vector<std::vector<T>> get_matrix(int64_t matrix_id) {
        std::vector<std::vector<T>> result;
        auto query_result = _sess->execute(scd_statement(fmt::format(fetch_whole_matrix_query, "row", table_name_rows), 1).bind(matrix_id));
        while(query_result.next_row()) {
            int64_t row = query_result.get_column<int64_t>("row");
        }
    }


private:
    int32_t get_new_matrix_id() {
        auto res = _sess->execute(get_max_matrix_id_query);
        if(!res.next_row() || res.is_column_null("id")) return 0;
        return res.get_column<int32_t>("id");
    }

    std::set<int64_t> get_row_list(int32_t matrix_id) {
        auto query_result = _sess->execute(scd_statement(matrix_fetch_row_list, 1).bind(matrix_id));
        std::set<int64_t> rows;
        while(query_result.next_row()) {
            auto rownum = query_result.get_column<int64_t>("row");
            rows.insert(rownum);
        }

        return rows;
    }

    std::set<int64_t> generate_row_matrix(int32_t id, matrix_value_generator<T>&& gen) {
        std::vector<char> blanks(columns_in_row * 2, '?');
        scd_prepared_query insert_row_prepared = _sess->prepare(fmt::format(matrix_insert_row_query, fmt::join(_data_columns, ", "), fmt::join(blanks, ", ")));

        std::set<int64_t> columns;
        std::set<std::tuple<int64_t, double>> row_data;
        int64_t row = 1;
        int64_t part = 0;

        while (gen.has_next()) {
            matrix_value<T> next = gen.next();
            //fmt::print(stderr, "Next cell: [{}, {}] : {}\n", next.i, next.j, next.val);
            columns.insert(next.j);
            if(next.i != row || row_data.size() == columns_in_row) {
                //fmt::print(stderr, "row: {}. part: {}, size: {}, next.i: {}\n", row, part, row_data.size(), next.i);
                auto stmt = insert_row_prepared.get_statement();
                stmt.bind(id, row, part, (int32_t)row_data.size());
                for(auto entry : row_data) {
                    stmt.bind(std::get<0>(entry), std::get<1>(entry));
                }
                _sess->execute(stmt);
                row_data.clear();
                part = (next.i == row) ? part + 1 : 0;
                row = next.i;
            }
            row_data.emplace(next.j, next.val);

        }

        if(!row_data.empty()) {
            auto stmt = insert_row_prepared.get_statement();
            stmt.bind(id, (int64_t)row, part, (int32_t)row_data.size());
            for(auto entry : row_data) {
                stmt.bind(std::get<0>(entry), std::get<1>(entry));
            }
            _sess->execute(stmt);
            row_data.clear();
        }

        return columns;
    }

    void create_column_matrix(int32_t id, const std::set<int64_t>& columns) {
        scd_prepared_query init_column_prepared = _sess->prepare(matrix_init_column_part);
        scd_prepared_query fetch_row_part_prepared = _sess->prepare(matrix_fetch_row_part);

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
                auto result = _sess->execute(fetch_row_part_prepared.get_statement().bind(id, rownum, part));
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
                    fmt::print(stmt_str);
                    info->second.second++;
                    _sess->execute(scd_statement(stmt_str, 6).bind(info->second.second, rownum, (double)value, id, column, info->second.first));
                }
                part++;
            } while (true);
        }
    }
};

int main(int argc, char *argv[]) {
    using namespace std::string_literals;
    std::shared_ptr<scd_session> conn;
    try {
        conn = std::make_shared<scd_session>(argv[1]);
        fmt::print("Connected\n");
    } catch (...) {
        fmt::print("Connection error\n");
        exit(1);
    }
    argv++;

    auto multiplicator = std::make_unique<LIL<float>>(conn);


    if(argv[1] == "make_tables"s) {
        multiplicator->create_tables();
        fmt::print("Tables dropped and re-created\n");

    } else if(argv[1] == "list"s) {
        fmt::print("Matrix list:\n");
        auto result = multiplicator->fetch_list();
        for(auto matrix_info : result) {
            auto [id, height, width] = matrix_info;
            fmt::print("id: {} height: {}, width: {}\n", id, height, width);
        }

    } else if(argv[1] == "gen"s) {
        size_t height = std::stoull(argv[2]);
        size_t width = std::stoull(argv[3]);
        size_t values = std::stoull(argv[4]);
        std::shared_ptr factory = std::make_shared<float_value_factory>(0.0, 100.0, 0);
        size_t id = multiplicator->load_matrix(sparse_matrix_value_generator<float>(height, width, values, 1, factory));
        fmt::print("Matrix created, id: {}\n", id);

    } else if(argv[1] == "delete"s) {
        size_t id = std::stoull(argv[2]);
        multiplicator->delete_matrix(id);
        fmt::print("Deleted matrix {}\n", id);

    } else if(argv[1] == "show"s) {

    } else if(argv[1] == "multiply"s) {
        size_t a = std::stoull(argv[2]);
        size_t b = std::stoull(argv[3]);
        size_t c = multiplicator->multiply(a, b);
        fmt::print("Multiplication finished, result id: {}\n", c);

    }

}