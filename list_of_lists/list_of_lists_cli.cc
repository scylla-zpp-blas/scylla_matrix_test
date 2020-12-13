#include <memory>
#include <fmt/format.h>

#include "list_of_lists.hh"

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
    argc--;

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
        int seed = 1337;
        if(argc > 5) {
            seed = std::stoi(argv[5]);
        }
        std::shared_ptr factory = std::make_shared<float_value_factory>(0.0, 100.0, seed);
        size_t id = multiplicator->load_matrix(sparse_matrix_value_generator<float>(height, width, values, seed * 2, factory));
        fmt::print("Matrix created, id: {}\n", id);

    } else if(argv[1] == "delete"s) {
        int32_t id = std::stoi(argv[2]);
        multiplicator->delete_matrix(id);
        fmt::print("Deleted matrix {}\n", id);

    } else if(argv[1] == "show"s) {
        auto result = multiplicator->get_matrix_as_dense(std::stoi(argv[2]));
        for(auto row : result) {
            fmt::print("[ {:6.3f} ]\n", fmt::join(row, ", "));
        }

    } else if(argv[1] == "multiply"s) {
        size_t a = std::stoull(argv[2]);
        size_t b = std::stoull(argv[3]);
        size_t c = multiplicator->multiply(a, b);
        fmt::print("Multiplication finished, result id: {}\n", c);

    }

}