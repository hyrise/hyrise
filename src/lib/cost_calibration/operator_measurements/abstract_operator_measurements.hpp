#include "string"
#include "operators/abstract_operator.hpp"

namespace opossum {
    class AbstractOperatorMeasurements {
    public:
        AbstractOperatorMeasurements(std::shared_ptr<const AbstractOperator> op);

        std::vector<const std::string> as_vector() const;
        std::vector<const std::string> get_header_as_vector();

        void export_to_csv(const std::string& path_to_csv) const;

    private:
        const std::shared_ptr<const AbstractOperator> _op; //TODO Remove const from shared_ptr

        //TODO change vector to shared_ptr of an vector
        void vector_to_csv(std::vector<const std::string> vector, const std::string& path_to_csv, const std::string& delimiter) const;

        std::string get_input_rows_left() const;
        std::string get_input_rows_right() const;
        std::string get_output_rows() const;
        std::string get_runtime_ns() const;
    };
}  // namespace opossum