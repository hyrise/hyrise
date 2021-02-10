#pragma once

#include <boost/hana/tuple.hpp>

#include "calibration_table_wrapper.hpp"
#include "expression/expression_functional.hpp"
#include "logical_query_plan/abstract_lqp_node.hpp"
#include "logical_query_plan/stored_table_node.hpp"
#include "operators/abstract_operator.hpp"
#include "optimizer/optimizer.hpp"

namespace opossum {
class CalibrationLQPGenerator {
 public:
  CalibrationLQPGenerator();
  void generate(OperatorType operator_type, const std::shared_ptr<const CalibrationTableWrapper>& table);
  void generate_joins(std::vector<std::shared_ptr<const CalibrationTableWrapper>>& tables);
  void generate_aggregates(const std::vector<std::shared_ptr<const CalibrationTableWrapper>>& table_wrappers);
  const std::vector<std::shared_ptr<AbstractLQPNode>>& lqps();

 private:
  using ColumnPair = std::pair<const std::string, const std::string>;
  void _generate_table_scans(const std::shared_ptr<const CalibrationTableWrapper>& table_wrapper);
  void _generate_column_vs_column_scans(const std::shared_ptr<const CalibrationTableWrapper>& table_wrapper);
  [[nodiscard]] std::vector<CalibrationLQPGenerator::ColumnPair> _get_column_pairs(
      const std::shared_ptr<const CalibrationTableWrapper>& table_wrapper) const;
  std::shared_ptr<const CalibrationTableWrapper> _generate_semi_join_build_table(const size_t row_count) const;
  void _generate_semi_joins(const std::shared_ptr<const CalibrationTableWrapper>& left,
                            const std::shared_ptr<const CalibrationTableWrapper>& right);

  template <typename ColumnDataType>
  std::shared_ptr<PredicateNode> _get_predicate_node_based_on(const std::shared_ptr<LQPColumnExpression>& column,
                                                              const ColumnDataType& lower_bound,
                                                              const std::shared_ptr<AbstractLQPNode>& base);

  std::vector<std::shared_ptr<AbstractLQPNode>> _generated_lqps;
  const std::shared_ptr<Optimizer> _optimizer = std::make_shared<Optimizer>();

  // feature flags for the LQPGeneration
  static constexpr bool _enable_like_predicates = true;
  static constexpr bool _enable_reference_scans = true;
  static constexpr bool _enable_column_vs_column_scans = true;
  static constexpr bool _enable_between_predicates = true;
};
}  // namespace opossum
