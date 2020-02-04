#pragma once

#include "string"
#include <logical_query_plan/abstract_lqp_node.hpp>
#include "logical_query_plan/stored_table_node.hpp"
#include <string>
#include <expression/expression_functional.hpp>

#include "calibration_table_wrapper.hpp"
#include "logical_query_plan/abstract_lqp_node.hpp"
#include "logical_query_plan/stored_table_node.hpp"

namespace opossum{
class LQPGenerator {
 public:
  std::vector<std::shared_ptr<AbstractLQPNode>> generate(OperatorType operator_type, std::shared_ptr<const CalibrationTableWrapper> table) const;

 private:
  std::vector<std::shared_ptr<AbstractLQPNode>> _generate_table_scans(const std::shared_ptr<const CalibrationTableWrapper>& table) const;

    const std::vector<std::shared_ptr<AbstractLQPNode>> _generate_joins(std::shared_ptr<const CalibrationTableWrapper> sharedPtr) const;

    void _generate_column_vs_column_scans(const std::shared_ptr<const CalibrationTableWrapper> &sharedPtr) const;

    void _generate_column_vs_column_scans(const std::shared_ptr<const CalibrationTableWrapper> &table,
                                          std::vector<std::shared_ptr<AbstractLQPNode>> &lqp_queue) const;
};
}