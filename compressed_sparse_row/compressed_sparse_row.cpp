#include <cassandra.h>
#include <iostream>
#include <cstdlib>
#include <memory>
#include <string>
#include <random>
#include <map>
#include "../multiplicator.h"
#include "../utils/connector.h"
#include "../utils/requestor.h"
#include "../float_value_factory.hh"
#include "../sparse_matrix_value_generator.hh"

static int dimension = 5;

template<typename T>
class CSR : public multiplicator<T> {
    const std::string _namespace = "zpp";
    const std::string _table_name_values = "csr_test_matrix_values";
    const std::string _table_name_rows = "csr_test_matrix_rows";
    std::shared_ptr<connector> _conn;
    size_t _matrix_id;
    size_t result_id = 100;

    int get_row_begin(int row_num, int matrix_id) {
        requestor query_row_begin(_conn);
        int row_begin = -1;

        query_row_begin << "SELECT * FROM " << _namespace << "." << _table_name_rows
                        << " WHERE matrix_id=" << matrix_id << " AND row=" << row_num << ";";
        query_row_begin.send();
        if (query_row_begin.next_row()) {
            const CassValue* value_index = query_row_begin.get_column("idx");
            cass_value_get_int32(value_index, &row_begin);
        }
        return row_begin;
    }

    std::vector<matrix_value<T>> get_row(int i, int matrix_id) {
        int row_begin = get_row_begin(i, matrix_id);
        int row_end = get_row_begin(i+1, matrix_id);

        std::vector<matrix_value<T>> values;
        requestor query_values(_conn);
        query_values << "SELECT * FROM " << _namespace << "." << _table_name_values
                << " WHERE matrix_id=" << matrix_id << " AND idx>=" << row_begin << " AND idx<" << row_end << ";";
        query_values.send();
        while (query_values.next_row()) {
            const CassValue* value_col = query_values.get_column("column");
            const CassValue* value_val = query_values.get_column("value");
            int j;
            T val;
            cass_value_get_int32(value_col, &j);
            cass_value_get_float(value_val, &val);
            values.emplace_back(i, j, val);
        }
        return values;
    }

public:
    CSR(std::shared_ptr<connector> conn) : _conn(conn), _matrix_id(0) {
        /* Make sure that the necessary namespaces and table exist */

        requestor namespace_query(_conn);
        namespace_query << "CREATE KEYSPACE IF NOT EXISTS " << _namespace << " WITH REPLICATION = {"
                           "    'class' : 'SimpleStrategy',"
                           "    'replication_factor' : 1"
                           "};";
        namespace_query.send();

        requestor table_erase_values(_conn);
        table_erase_values << "DROP TABLE IF EXISTS " << _namespace << "." << _table_name_values << ";";
        table_erase_values.send();

        requestor table_erase_rows(_conn);
        table_erase_rows << "DROP TABLE IF EXISTS " << _namespace << "." << _table_name_rows << ";";
        table_erase_rows.send();


        requestor table_query_values(_conn);
        table_query_values << "CREATE TABLE " << _namespace << "." << _table_name_values << " ("
                       "    matrix_id int, "
                       "    idx int, "
                       "    column int, "
                       "    value float, "
                       "    PRIMARY KEY (matrix_id, idx) "
                       ") WITH CLUSTERING ORDER BY (idx ASC);";
        table_query_values.send();

        requestor table_query_rows(_conn);
        table_query_rows << "CREATE TABLE " << _namespace << "." << _table_name_rows << " ("
                       "    matrix_id int, "
                       "    row int, "
                       "    idx int, "
                       "    PRIMARY KEY (matrix_id, row) "
                       ") WITH CLUSTERING ORDER BY (row ASC);";
        table_query_rows.send();
    }

    void load_matrix(matrix_value_generator<T>&& gen) {
        _matrix_id++;
        size_t last_row = 0;
        size_t generated = 0;
        while(gen.has_next()) {
            auto mx_val = gen.next();
            requestor query(_conn);
            query << "INSERT INTO " << _namespace << "." << _table_name_values << " (matrix_id, idx, column, value) VALUES("
                  << _matrix_id << ", " << generated << ", " << mx_val.j << ", " << mx_val.val << ");\n";
            query.send();
            while (last_row < mx_val.i) {
                last_row++;
                requestor query_rows(_conn);
                query_rows << "INSERT INTO " << _namespace << "." << _table_name_rows << " (matrix_id, row, idx) VALUES("
                           << _matrix_id << ", " << last_row << ", " << generated << ");\n";
                query_rows.send();
            }
            generated++;
        }
        while (last_row <= dimension) {
            last_row++;
            requestor query_rows(_conn);
            query_rows << "INSERT INTO " << _namespace << "." << _table_name_rows << " (matrix_id, row, idx) VALUES("
                       << _matrix_id << ", " << last_row << ", " << generated << ");\n";
            query_rows.send();
        }
    }

    /* Multiplies two matrices loaded into Scylla with load_matrix. */
    void multiply() {
        size_t first_id = _matrix_id - 1;
        size_t second_id = _matrix_id;
        int curr_elems = 0;

        for (int row = 1; row <= dimension; row++) {
            std::map<int, matrix_value<T>> row_result;
            requestor query_rows(_conn);
            query_rows << "INSERT INTO " << _namespace << "." << _table_name_rows << " (matrix_id, row, idx) VALUES("
                       << result_id << ", " << row << ", " << curr_elems << ");\n";
            query_rows.send();

            std::vector<matrix_value<T>> row_first = get_row(row, first_id);
            for (auto val_1 : row_first) {
                std::vector<matrix_value<T>> row_second = get_row(val_1.j, second_id);
                for (auto val_2 : row_second) {
                    T added = val_1.val * val_2.val;
                    if (row_result.count(val_2.j)) {
                        auto & elem = row_result.at(val_2.j);
                        elem.val += added;
                    } else {
                      row_result.insert( std::pair<int,matrix_value<T>>(val_2.j, matrix_value<T>(row, val_2.j, added)));
                    }
                }
            }
            for (auto elem_res : row_result) {
                matrix_value<T> val_res = elem_res.second;
                requestor query(_conn);
                query << "INSERT INTO " << _namespace << "." << _table_name_values << " (matrix_id, idx, column, value) VALUES("
                      << result_id << ", " << curr_elems << ", " << val_res.j << ", " << val_res.val << ");\n";
                query.send();
                curr_elems++;
            }
        }
        requestor last_query_rows(_conn);
        last_query_rows << "INSERT INTO " << _namespace << "." << _table_name_rows << " (matrix_id, row, idx) VALUES("
                        << result_id << ", " << dimension+1 << ", " << curr_elems << ");\n";
        last_query_rows.send();
    }

    /* Obtains the value in the multiplication result at (x; y) = (pos.first; pos.second) */
    T get_result(std::pair<size_t, size_t> pos) {
        std::vector<matrix_value<T>> row = get_row(pos.first, result_id);
        for (auto val : row) {
            if (val.j == pos.second) {
                return val.val;
            }
        }
        return 0;
    }
};

int main(int argc, char* argv[]) {
    std::shared_ptr<connector> conn;

    try {
        conn = std::make_shared<connector>("172.17.0.2");
        std::cout << "Connected" << std::endl;
    } catch (...) {
        std::cout << "Connection error" << std::endl;
        exit(1);
    }

    std::shared_ptr factory = std::make_shared<float_value_factory>(0.0, 100.0, 0);
    CSR<float> multiplicator_instance(conn);

    multiplicator_instance.load_matrix(sparse_matrix_value_generator<float>(dimension, dimension, 15, 0, factory));
    multiplicator_instance.load_matrix(sparse_matrix_value_generator<float>(dimension, dimension, 15, 1, factory));

    multiplicator_instance.multiply();

    for (int i = 1; i <= dimension; i++) {
        for (int j = 1; j <= dimension; j++) {
            auto result = multiplicator_instance.get_result({i, j});
            if (result != 0) {
                std::cout << "(" << i << ", " << j << "): "  << result << std::endl;
            }
        }
    }

    return 0;
}
