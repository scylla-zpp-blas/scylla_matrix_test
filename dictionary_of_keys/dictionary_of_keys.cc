// Connecting to ScyllaDB with a simple C++ program
#include <cassandra.h>
#include <iostream>
#include <cstdlib>
#include <memory>
#include <string>
#include <random>
#include "../multiplicator.h"
#include "../utils/connector.h"
#include "../utils/requestor.h"
#include "../float_value_factory.hh"
#include "../sparse_matrix_value_generator.hh"

static int dimension = 5;

template<typename T>
class COO : public multiplicator<T> {
    const std::string _namespace = "zpp";
    const std::string _table_name = "coo_test_matrix";
    const int _block_size = 3;
    std::shared_ptr<connector> _conn;
    size_t _matrix_id;
    size_t result_id = 100;

    void submit_block(std::vector<matrix_value<T>>& block, size_t block_id, size_t matrix_id) {
        if (block.empty()) return;

        requestor query(_conn);
        query << "INSERT INTO " << _namespace << "." << _table_name << " (block_id, matrix_id, vals) "
                                                                       "   VALUES (" << block_id  << ", " << matrix_id << ", {";

        auto last = block.back();
        block.pop_back();

        for (auto value : block) {
            query << "(" << value.i << ", " << value.j << ", " << value.val << "), ";
        }
        query << "(" << last.i << ", " << last.j << ", " << last.val << ")});";

        query.send();
    }

    std::vector<matrix_value<T>> get_block(size_t block_id, size_t matrix_id) {
        requestor query(_conn);
        query << "SELECT * FROM " << _namespace << "." << _table_name
              << " WHERE block_id=" << block_id << " AND matrix_id=" << matrix_id << ";";
        query.send();

        std::vector<matrix_value<float>> ret;

        if (query.next_row()) {
            const CassValue* value_set = query.get_column("vals");
            CassIterator* set_iterator = cass_iterator_from_collection(value_set);
            while (cass_iterator_next(set_iterator)) {
                const CassValue *value_i, *value_j, *value_val;
                int i, j; double val;

                const CassValue* value_tuple = cass_iterator_get_value(set_iterator);
                CassIterator* tuple_iterator = cass_iterator_from_tuple(value_tuple);

                cass_iterator_next(tuple_iterator);
                cass_value_get_int32(cass_iterator_get_value(tuple_iterator), &i);

                cass_iterator_next(tuple_iterator);
                cass_value_get_int32(cass_iterator_get_value(tuple_iterator), &j);

                cass_iterator_next(tuple_iterator);
                cass_value_get_double(cass_iterator_get_value(tuple_iterator), &val);

                cass_iterator_free(tuple_iterator);

                ret.emplace_back(i, j, val);
            }
            cass_iterator_free(set_iterator);
        }

        return ret;
    }

    size_t div_up(size_t val, size_t divisor) {
        return (val - 1) / divisor + 1;
    }

    size_t get_block_index_for_cell(std::pair<size_t, size_t> pos) {
        size_t first = div_up(pos.first, _block_size);
        size_t second = div_up(pos.second, _block_size);
        return div_up(dimension, _block_size) * (first - 1) + second;
    }
public:
    COO(std::shared_ptr<connector> conn) : _conn(conn), _matrix_id(0) {
        /* Make sure that the necessary namespaces and table exist */

        requestor namespace_query(_conn);
        namespace_query << "CREATE KEYSPACE IF NOT EXISTS " << _namespace << " WITH REPLICATION = {"
                                                                             "    'class' : 'SimpleStrategy',"
                                                                             "    'replication_factor' : 1"
                                                                             "};";
        namespace_query.send();

        requestor table_erase(_conn);
        table_erase << "DROP TABLE " << _namespace << "." << _table_name << ";";
        table_erase.send();

        requestor table_query(_conn);
        table_query << "CREATE TABLE " << _namespace << "." << _table_name << " ("
                                                                              "    block_id int, "
                                                                              "    matrix_id int, "
                                                                              "    vals set<frozen<tuple<int, int, double>>>, "
                                                                              "    PRIMARY KEY (block_id, matrix_id) "
                                                                              ") WITH CLUSTERING ORDER BY (matrix_id ASC);";
        table_query.send();
    }

    void load_matrix(matrix_value_generator<T>&& gen) {
        _matrix_id++;

        int next_batch_start = _block_size + 1;
        size_t block_id = 0;
        std::vector< std::vector<matrix_value<T>> > blocks(div_up(dimension, _block_size));

        std::cerr << gen.has_next() << std::endl;

        while (gen.has_next()) {
            matrix_value<T> next = gen.next();
            next.i++; next.j++; // generator generates fields from 0 to dimension-1
            std::cout << next.i << " " << next.j << " " << gen.has_next() << std::endl;
            while (next.i >= next_batch_start) {
                for (auto &b : blocks) {
                    submit_block(b, ++block_id, _matrix_id);
                    b.clear();
                }

                next_batch_start += _block_size;
            }

            blocks[div_up(next.j, _block_size) - 1].push_back(next);
        }

        /* Submit the remainder */
        for (auto &b : blocks) {
            submit_block(b, ++block_id, _matrix_id);
            b.clear();
        }
    }

    /* Multiplies two matrices loaded into Scylla with load_matrix. */
    void multiply() {
        size_t blocks_wide = div_up(dimension, _block_size);
        size_t blocks_high = div_up(dimension, _block_size);

        for (size_t i = 1; i <= blocks_high; i++) {
            for (size_t j = 1; j <= blocks_wide; j++) {
                size_t block_id = (i - 1) * blocks_wide + j;
                auto copy_from_a = get_block(block_id, _matrix_id - 1);
                submit_block(copy_from_a, block_id, result_id);
            }
        }
    }

    /* Obtains the value in the multiplication result at (x; y) = (pos.first; pos.second) */
    T get_result(std::pair<size_t, size_t> pos) {
        size_t block_id = get_block_index_for_cell(pos);
        std::cerr << "Block for cell: " << pos.first << " " << pos.second << ": " << block_id << std::endl;

        auto target_block = get_block(block_id, result_id);
        for (auto& val : target_block) {
            if (val.i == pos.first && val.j == pos.second) {
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
    COO<float> multiplicator_instance(conn);

    multiplicator_instance.load_matrix(sparse_matrix_value_generator<float>(dimension, dimension, 3, 0, factory));
    multiplicator_instance.load_matrix(sparse_matrix_value_generator<float>(dimension, dimension, 3, 1, factory));

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