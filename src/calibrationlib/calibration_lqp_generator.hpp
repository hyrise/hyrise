#pragma once

#include "string"

#include <boost/hana/tuple.hpp>

#include <expression/expression_functional.hpp>
#include <logical_query_plan/abstract_lqp_node.hpp>
#include <operators/abstract_operator.hpp>

#include "calibration_table_wrapper.hpp"
#include "logical_query_plan/stored_table_node.hpp"
#include "operators/join_hash.hpp"
#include "operators/join_nested_loop.hpp"
#include "operators/join_sort_merge.hpp"

namespace opossum {
class CalibrationLQPGenerator {
 public:
  CalibrationLQPGenerator();
  void generate(OperatorType operator_type, const std::shared_ptr<const CalibrationTableWrapper>& table);
  void generate_joins(const std::vector<std::shared_ptr<const CalibrationTableWrapper>>& tables);
  const std::vector<std::shared_ptr<AbstractLQPNode>>& get_lqps();

 private:
  using ColumnPair = std::pair<const std::string, const std::string>;
  void _generate_table_scans(const std::shared_ptr<const CalibrationTableWrapper>& table_wrapper);
  void _generate_column_vs_column_scans(const std::shared_ptr<const CalibrationTableWrapper>& table_wrapper);
  [[nodiscard]] std::vector<CalibrationLQPGenerator::ColumnPair> _get_column_pairs(
      const std::shared_ptr<const CalibrationTableWrapper>& table_wrapper) const;
  void _generate_index_scans(const std::shared_ptr<const CalibrationTableWrapper>& table_wrapper);
  void _generate_joins(const std::shared_ptr<const CalibrationTableWrapper>& left_table,
                       const std::shared_ptr<const CalibrationTableWrapper>& right);

  std::vector<std::shared_ptr<AbstractLQPNode>> _generated_lqps;

  // feature flags for the LQPGeneration
  static constexpr bool _enable_like_predicates = true;
  static constexpr bool _enable_reference_scans = true;
  static constexpr bool _enable_index_scans = true;
  static constexpr bool _enable_column_vs_column_scans = true;
  static constexpr bool _enable_between_predicates = true;
  static constexpr bool _enable_reference_joins = true;
  static constexpr auto _join_operators = hana::to_tuple(hana::tuple_t<JoinHash>);  //, JoinSortMerge, JoinNestedLoop>);
  const std::vector<JoinType> _join_types = {JoinType::Hash};  //, JoinType::SortMerge, JoinType::NestedLoop};
};
}  // namespace opossum
