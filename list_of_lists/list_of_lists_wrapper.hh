#include "list_of_lists.hh"

template<typename T>
class LIL_wrapper : public multiplicator<T> {
private:
    LIL<T> repr;
    size_t a = 0, b = 0, c = 0;
    std::vector<std::vector<T>> result;
    bool first_call = true;
public:

    explicit LIL_wrapper(const std::shared_ptr<scmd::session>& connection) : repr(connection) {};

    void load_matrix(matrix_value_generator<T>&& gen) override {
        if(first_call) {
            repr.create_tables();
            this->a = repr.load_matrix(std::move(gen));
            first_call = false;
        } else {
            this->b = repr.load_matrix(std::move(gen));
        }
    };

    void multiply() override {
        this->c = repr.multiply(this->a, this->b);
        this->result = repr.get_matrix_as_dense(this->c);
    };

    T get_result(std::pair<size_t, size_t> pos) override {
        return result[pos.first - 1][pos.second - 1];
    };

    ~LIL_wrapper() = default;
};
