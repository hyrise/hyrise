#include "cost_model_coefficient_reader.hpp"

namespace opossum {

//    const OperatorType operator_type;
//    const DataType data_type;
//    const bool is_reference_segment;
//    const bool is_small_table;

    const ModelCoefficientsPerGroup CostModelCoefficientReader::read_coefficients(const std::string& file_path) {
        return {
                {TableScanModelGroup{OperatorType::TableScan, DataType::Int, false, false}, {{"", 0.0}}}
        };
    }
}  // namespace opossum
