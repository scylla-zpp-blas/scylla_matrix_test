#include "list_of_lists.hh"

template<typename T>
class LIL_wrapper : public multiplicator<T> {
private:
    LIL<T> repr;
    size_t a, b, c;
    std::vector<std::vector<T>> result;
public:

    explicit LIL_wrapper(const std::shared_ptr<scd_session>& connection) : repr(connection) {};

    void load_matrix(matrix_value_generator<T>&& gen) override {
        static bool first_call = true;
        if(first_call) {
            this.a = repr.load_matrix(gen);
            first_call = false;
        } else {
            this.b = repr.load_matrix(gen);
        }
    };

    void multiply() override {
        this.c = repr.multiply(this.a, this.b);
        this-result = repr.get_matrix_as_dense(this.c);
    };

    T get_result(std::pair<size_t, size_t> pos) override {
        return result[pos.first][pos.second];
    };

    ~multiplicator() {};
};


template LIL_wrapper<float>;