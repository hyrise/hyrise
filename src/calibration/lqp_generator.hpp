#pragma once

#include "string"

#include <logical_query_plan/abstract_lqp_node.hpp>
#include <expression/expression_functional.hpp>

#include "logical_query_plan/stored_table_node.hpp"
#include "calibration_table_wrapper.hpp"

namespace opossum {
class LQPGenerator {
 public:
    LQPGenerator();
    void generate(OperatorType operator_type, const std::shared_ptr<const CalibrationTableWrapper> &table);
    const std::vector<std::shared_ptr<AbstractLQPNode>> &get_lqps();

 private:
    using ColumnPair = std::pair<const std::string, const std::string>;
    void _generate_table_scans(const std::shared_ptr<const CalibrationTableWrapper> &table);
    void _generate_joins(const std::shared_ptr<const CalibrationTableWrapper> &sharedPtr);
    void _generate_column_vs_column_scans(const std::shared_ptr<const CalibrationTableWrapper> &table);
    [[nodiscard]] std::vector<LQPGenerator::ColumnPair> _get_column_pairs(
            const std::shared_ptr<const CalibrationTableWrapper> &table) const;

    std::vector<std::shared_ptr<AbstractLQPNode>> _generated_lpqs;

    // feature flags for the LQPGeneration
    const bool _enable_like_predicates = true;
    const bool _enable_reference_scans = true;
    const bool _enable_column_vs_column_scans = true;
    const bool _enable_between_predicates = true;
};
}  // namespace opossum
