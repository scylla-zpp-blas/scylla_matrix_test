// Connecting to ScyllaDB with a simple C++ program
#include "dictionary_of_keys.hh"

int main(int argc, char* argv[]) {
    std::shared_ptr<connector> conn;

    try {
        conn = std::make_shared<connector>("172.19.0.2");
        std::cout << "Connected" << std::endl;
    } catch (...) {
        std::cout << "Connection error" << std::endl;
        exit(1);
    }

    std::shared_ptr factory = std::make_shared<float_value_factory>(0.0, 100.0, 2137);
    DOK<float> multiplicator_instance(conn);

    size_t DIMENSION = 2000;
    multiplicator_instance.load_matrix(sparse_matrix_value_generator<float>(DIMENSION, DIMENSION, 50, 2137, factory));
    multiplicator_instance.load_matrix(sparse_matrix_value_generator<float>(DIMENSION, DIMENSION, 50, 2138, factory));

    multiplicator_instance.multiply();

    multiplicator_instance.print_all();
    return 0;
}
