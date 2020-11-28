// Connecting to ScyllaDB with a simple C++ program
#include <cassandra.h>
#include <iostream>
#include <cstdlib>
#include <memory>
#include <string>
#include <random>
#include <optional>
#include "../multiplicator.h"
#include "../utils/connector.h"
#include "../utils/requestor.h"
#include "../float_value_factory.hh"
#include "../sparse_matrix_value_generator.hh"

template<typename T>
class dok : public multiplicator<T> {
private:
    const std::string _KEYSPACE_NAME = "zpp";
    const std::string _TABLE_NAME = "dok_test_matrix";
    const size_t _BLOCK_SIZE = 1000;
    std::shared_ptr<connector> _conn;
    size_t _matrix_id;
    size_t result_id = 100;
    size_t _first_matrix_height, _first_matrix_width, _second_matrix_height, _second_matrix_width;

    void submit_block(const std::vector<matrix_value<T>>& block, size_t matrix_id) {
        if (block.empty()) return;

        for (auto val: block) {
            requestor query(_conn);
            query << "INSERT INTO " << _KEYSPACE_NAME << "." << _TABLE_NAME << " (matrix_id, pos_y, pos_x, val) "
                                                                               "   VALUES ("
                                                                               << matrix_id << ", "
                                                                               << val.i << ", "
                                                                               << val.j << ", "
                                                                               << val.val << ");";
            query.send();
        }
    }

    inline std::optional<matrix_value<T>> fetch_next_coords(size_t min_pos_y, size_t matrix_id) {
        requestor query(_conn);
        query << "SELECT * FROM " << _KEYSPACE_NAME << "." << _TABLE_NAME
              << " WHERE pos_y>=" << min_pos_y << " AND matrix_id=" << matrix_id
              << " LIMIT " << 1 << ";";
        query.send();

        if (query.next_row()) {
            const CassValue* _cass_pos_x = query.get_column("pos_x");
            const CassValue* _cass_pos_y = query.get_column("pos_y");
            const CassValue* _cass_val = query.get_column("val");
            cass_int64_t _pos_x, _pos_y;
            cass_double_t _val;

            cass_value_get_int64(_cass_pos_x, &_pos_x);
            cass_value_get_int64(_cass_pos_y, &_pos_y);
            cass_value_get_double(_cass_val, &_val);

            return std::make_optional(matrix_value<T>(_pos_y, _pos_x, _val));
        }

        return std::nullopt;
    }

    inline std::optional<matrix_value<T>> fetch_next_coords(size_t min_pos_x, size_t pos_y, size_t matrix_id) {
        requestor query(_conn);
        query << "SELECT * FROM " << _KEYSPACE_NAME << "." << _TABLE_NAME
              << " WHERE pos_x>=" << min_pos_x << " AND pos_y=" << pos_y << " AND matrix_id=" << matrix_id
              << " LIMIT " << 1 << ";";
        query.send();

        if (query.next_row()) {
            const CassValue* _cass_pos_x = query.get_column("pos_x");
            const CassValue* _cass_pos_y = query.get_column("pos_y");
            const CassValue* _cass_val = query.get_column("val");
            cass_int64_t _pos_x, _pos_y;
            cass_double_t _val;

            cass_value_get_int64(_cass_pos_x, &_pos_x);
            cass_value_get_int64(_cass_pos_y, &_pos_y);
            cass_value_get_double(_cass_val, &_val);

            return std::make_optional(matrix_value<T>(_pos_y, _pos_x, _val));
        }

        return std::nullopt;
    }

    inline std::vector<matrix_value<T>> get_block(size_t min_pos_x, size_t pos_y, size_t matrix_id) {
        requestor query(_conn);
        query << "SELECT * FROM " << _KEYSPACE_NAME << "." << _TABLE_NAME
              << " WHERE pos_x>=" << min_pos_x << " AND pos_y=" << pos_y << " AND matrix_id=" << matrix_id
              << " LIMIT " << _BLOCK_SIZE << ";";
        query.send();

        std::vector<matrix_value<T>> ret;

        while (query.next_row()) {
            const CassValue* _cass_pos_x = query.get_column("pos_x");
            const CassValue* _cass_pos_y = query.get_column("pos_y");
            const CassValue* _cass_val = query.get_column("val");
            cass_int64_t _pos_x, _pos_y;
            cass_double_t _val;

            cass_value_get_int64(_cass_pos_x, &_pos_x);
            cass_value_get_int64(_cass_pos_y, &_pos_y);
            cass_value_get_double(_cass_val, &_val);

            ret.emplace_back(_pos_y, _pos_x, _val);
        }

        return ret;
    }

public:
    explicit dok(std::shared_ptr<connector> conn) : _conn(conn), _matrix_id(0) {
        /* Make sure that the necessary namespaces and table exist */

        requestor namespace_query(_conn);
        namespace_query << "CREATE KEYSPACE IF NOT EXISTS " << _KEYSPACE_NAME << " WITH REPLICATION = {"
                                                                                 "    'class' : 'SimpleStrategy',"
                                                                                 "    'replication_factor' : 1"
                                                                                 "};";
        namespace_query.send();

        try {
            requestor table_erase(_conn);
            table_erase << "DROP TABLE IF EXISTS " << _KEYSPACE_NAME << "." << _TABLE_NAME << ";";
            table_erase.send();
        }
        catch (std::runtime_error &e) {
            std::cerr << "Drop table error: ";
            std::cerr << e.what() << std::endl;
        }

        requestor table_query(_conn);
        table_query << "CREATE TABLE " << _KEYSPACE_NAME << "." << _TABLE_NAME << " ("
                                                                                  "    matrix_id int, "
                                                                                  "    pos_y bigint, "
                                                                                  "    pos_x bigint, "
                                                                                  "    val double, "
                                                                                  "    PRIMARY KEY (matrix_id, pos_y, pos_x) "
                                                                                  ") WITH CLUSTERING ORDER BY (pos_y ASC, pos_x ASC);";
        table_query.send();
    }

    void load_matrix(matrix_value_generator<T>&& gen) override {
        _matrix_id++;
        std::cerr << "Load matrix " << _matrix_id << std::endl;

        bool transpose;

        if (_matrix_id == 1) {
            _first_matrix_height = gen.height();
            _first_matrix_width = gen.width();
            transpose = false;
        }
        else {
            _second_matrix_height = gen.width();
            _second_matrix_width = gen.height();
            transpose = true;
        }

        std::vector<matrix_value<T>> _block;

        while (gen.has_next()) {
            matrix_value<T> _next = gen.next();
            std::cout << _next.i << " " << _next.j << " " << _next.val << " " << gen.has_next() << std::endl;

            if (transpose) {
                std::swap(_next.i, _next.j);
            }

            _block.push_back(_next);

            if (_block.size() >= _BLOCK_SIZE) {
                submit_block(_block, _matrix_id);
                _block.clear();
            }

        }
        submit_block(_block, _matrix_id);
    }

    // TODO wrap dynamically loaded vectors into abstraction, make this function simpler.
    /* Multiplies two matrices loaded into Scylla with load_matrix. */
    void multiply() override {
        size_t _width = std::min(_first_matrix_width, _second_matrix_width);
        size_t _height = std::min(_first_matrix_height, _second_matrix_height);

        // Find position of first non-zero row of first matrix.
        auto _f_start = fetch_next_coords(0, 1);
        while (_f_start.has_value()) {
            // Find position of first non-zero transposed column of second matrix.
            auto _s_start = fetch_next_coords(0, 2);
            while (_s_start.has_value()) {
                // Multiply row by transposed column.
                auto _f_block = get_block(_f_start->j, _f_start->i, 1);
                auto _s_block = get_block(_f_start->j, _s_start->i, 2);
                T _sum = 0;
                size_t i = 0, j = 0;

                while (!(_f_block.empty() or _s_block.empty())) {
                    while (i < _f_block.size() && _f_block[i].j <= _s_block[j].j) {
                        if (_f_block[i].j == _s_block[i].j) {
                            _sum += _f_block[i].val * _s_block[i].val;
                        }
                        i++;
                    }
                    if (i == _f_block.size()) {
                        _f_block = get_block(_f_block[i - 1].j + 1, _f_start->i, 1);
                        i = 0;
                        if (_f_block.empty()) {
                            break;
                        }
                    }
                    while (j < _s_block.size() && _s_block[j].j <= _f_block[i].j) {
                        if (_s_block[j].j == _f_block[i].j) {
                            _sum += _s_block[j].val * _f_block[i].val;
                        }
                        j++;
                    }
                    if (j == _s_block.size()) {
                        _s_block = get_block(_s_block[j - 1].j + 1, _s_start->i, 2);
                        j = 0;
                        if (_s_block.empty()) {
                            break;
                        }
                    }
                }

                std::vector<matrix_value<T>> _upload_block;
                _upload_block.emplace_back(_f_start->i, _s_start->i, _sum);
                submit_block(_upload_block, result_id);

                _s_start = fetch_next_coords(_s_start->i + 1, 2);
            }
            _f_start = fetch_next_coords(_f_start->i + 1, 1);
        }
    }

    /* Obtains the value in the multiplication result at (x; y) = (pos.first; pos.second) */
    T get_result(std::pair<size_t, size_t> pos) override {
        auto result = fetch_next_coords(pos.first, pos.second, result_id);
        if (result.has_value() && result->j == pos.first && result->i == pos.second) {
            return result->val;
        }
        return 0;
    }

    void print_all() {
        requestor query(_conn);
        query << "SELECT * FROM " << _KEYSPACE_NAME << "." << _TABLE_NAME
              << " WHERE matrix_id=" << result_id
              << " LIMIT " << _BLOCK_SIZE << ";";
        query.send();

        while (query.next_row()) {
            const CassValue* _cass_pos_x = query.get_column("pos_x");
            const CassValue* _cass_pos_y = query.get_column("pos_y");
            const CassValue* _cass_val = query.get_column("val");
            cass_int64_t _pos_x, _pos_y;
            cass_double_t _val;

            cass_value_get_int64(_cass_pos_x, &_pos_x);
            cass_value_get_int64(_cass_pos_y, &_pos_y);
            cass_value_get_double(_cass_val, &_val);

            std::cout << "(" << _pos_y << ", " << _pos_x << "): "  << _val << std::endl;
        }
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

    std::shared_ptr factory = std::make_shared<float_value_factory>(0.0, 100.0, 2137);
    dok<float> multiplicator_instance(conn);

    size_t DIMENSION = 2000;
    multiplicator_instance.load_matrix(sparse_matrix_value_generator<float>(DIMENSION, DIMENSION, 50, 2137, factory));
    multiplicator_instance.load_matrix(sparse_matrix_value_generator<float>(DIMENSION, DIMENSION, 50, 2138, factory));

    multiplicator_instance.multiply();

    multiplicator_instance.print_all();
    return 0;
}