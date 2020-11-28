#include <algorithm>
#include <cassandra.h>
#include <iostream>
#include <map>
#include <memory>
#include <string>
#include "../multiplicator.hh"
#include "../utils/connector.hh"
#include "../utils/requestor.hh"

#ifdef DEBUG
#define DBG(x) x
#else
#define DBG(x)
#endif

template<typename T>
class COO : public multiplicator<T> {
    using _block_t = std::vector<matrix_value<T>>;
    const std::string _namespace = "zpp";
    const std::string _table_name = "coo_test_matrix";
    const int _block_size = 64;
    const size_t _result_id = 100;
    std::shared_ptr<connector> _conn;
    size_t _matrix_id;
    size_t _dimension;

    void submit_block(_block_t& block, size_t block_id, size_t matrix_id) {
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

    _block_t get_block(size_t block_id, size_t matrix_id) {
        requestor query(_conn);
        query << "SELECT * FROM " << _namespace << "." << _table_name
              << " WHERE block_id=" << block_id << " AND matrix_id=" << matrix_id << ";";
        query.send();

        std::vector<matrix_value<float>> ret;

        if (query.next_row()) {
            const CassValue* value_set = query.get_column("vals");
            CassIterator* set_iterator = cass_iterator_from_collection(value_set);
            while (cass_iterator_next(set_iterator)) {
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
        return div_up(_dimension, _block_size) * (first - 1) + second;
    }

    void transpose_block(_block_t& b) {
        for (auto &cell : b) {
            std::swap(cell.i, cell.j);
        }

        /* Replace with bucket sort */
        std::sort(b.begin(), b.end(), [](auto a, auto b){
            return std::make_pair(a.i, a.j) < std::make_pair(b.i, b.j);
        });
    }
public:
    COO(std::shared_ptr<connector> conn) : _conn(conn), _matrix_id(0), _dimension(0) {
        /* Make sure that the necessary namespaces and table exist */

        requestor namespace_query(_conn);
        namespace_query << "CREATE KEYSPACE IF NOT EXISTS " << _namespace << " WITH REPLICATION = {"
                           "    'class' : 'SimpleStrategy',"
                           "    'replication_factor' : 1"
                           "};";
        namespace_query.send();

        requestor table_erase(_conn);
        table_erase << "DROP TABLE IF EXISTS " << _namespace << "." << _table_name << ";";
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
        if (gen.width() != gen.height() || _dimension != 0 && gen.width() != _dimension) {
            throw std::runtime_error("Wrong matrix size of " + std::to_string(gen.height()) + "x" + std::to_string(gen.width()));
        }

        _dimension = gen.width();
        _matrix_id++;

        int next_batch_start = _block_size + 1;
        size_t block_id = 0;
        std::vector<_block_t> blocks(div_up(_dimension, _block_size));

        DBG(std::cerr << "Generator has first number: " << gen.has_next() << std::endl;)

        while (gen.has_next()) {
            matrix_value<T> next = gen.next();
            DBG(std::cerr << "Next number in generator: (" << next.i << ", " << next.j << "), some are still left (T/F): " << gen.has_next() << std::endl;)
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
        size_t blocks_dimension = div_up(_dimension, _block_size);

        for (size_t i = 1; i <= blocks_dimension; i++) {
            for (size_t j = 1; j <= blocks_dimension; j++) {
                /* We can use a vector, but that will be a bit more difficult */
                std::map<std::pair<int, int>, double> result;

                for (size_t k = 1; k <= blocks_dimension; k++) {
                    _block_t copy_from_a = get_block((i - 1) * blocks_dimension + k, 1);
                    _block_t copy_from_b = get_block((k - 1) * blocks_dimension + j, 2);

                    /* Strategy: for every row of copy_from_a process all rows of copy_from_b */
                    auto it_1 = copy_from_a.begin();

                    /* Multiplication: (i, k) * (k, j) -> (i, j), summed over k = 1..dimension */
                    while (it_1 != copy_from_a.end()) {
                        size_t row = it_1->i;
                        auto it_2 = copy_from_b.begin();

                        while (it_1 != copy_from_a.end() && it_2 != copy_from_b.end() && it_1->i == row) {
                            if (it_1->j < it_2->i) {
                                it_1++;
                            } else if (it_1->j > it_2->i) {
                                it_2++;
                            } else {
                                /* it_1->j == it_2->i == k */
                                result[std::make_pair(it_1->i, it_2->j)] += it_1->val * it_2->val;
                                it_2++;
                            }
                        }

                        while (it_1 != copy_from_a.end() && it_1->i == row) {
                            it_1++;
                        }
                    }
                }

                _block_t result_block;
                for (auto a : result) {
                    result_block.emplace_back(a.first.first, a.first.second, a.second);
                }

                submit_block(result_block, (i - 1) * blocks_dimension + j, _result_id);
            }
        }
    }

    /* Obtains the value in the multiplication result at (x; y) = (pos.first; pos.second) */
    T get_result(std::pair<size_t, size_t> pos) {
        size_t block_id = get_block_index_for_cell(pos);

        DBG(std::cerr << "Block for cell: " << pos.first << " " << pos.second << ": " << block_id << std::endl;)

        auto target_block = get_block(block_id, _result_id);
        for (auto& val : target_block) {
            if (val.i == pos.first && val.j == pos.second) {
                return val.val;
            }
        }

        return 0;
    }
};
