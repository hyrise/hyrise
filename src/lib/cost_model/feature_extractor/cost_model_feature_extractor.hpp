#pragma once

#include <string>

#include "expression/abstract_predicate_expression.hpp"
#include "expression/pqp_column_expression.hpp"

#include "cost_model/feature/aggregate_features.hpp"
#include "cost_model/feature/column_features.hpp"
#include "cost_model/feature/constant_hardware_features.hpp"
#include "cost_model/feature/cost_model_features.hpp"
#include "cost_model/feature/join_features.hpp"
#include "cost_model/feature/projection_features.hpp"
#include "cost_model/feature/runtime_hardware_features.hpp"
#include "cost_model/feature/table_scan_features.hpp"

#include "logical_query_plan/abstract_lqp_node.hpp"
#include "logical_query_plan/aggregate_node.hpp"
#include "logical_query_plan/join_node.hpp"
#include "logical_query_plan/predicate_node.hpp"
#include "logical_query_plan/projection_node.hpp"

#include "storage/base_segment.hpp"
#include "storage/encoding_type.hpp"

namespace opossum {
namespace cost_model {

class CostModelFeatureExtractor {
 public:
  static const CostModelFeatures extract_features(const std::shared_ptr<const AbstractLQPNode>& node);

 private:
  static const CostModelFeatures _extract_general_features(const std::shared_ptr<const AbstractLQPNode>& node);
  static const ConstantHardwareFeatures _extract_constant_hardware_features();
  static const RuntimeHardwareFeatures _extract_runtime_hardware_features();

  static const TableScanFeatures _extract_features(const std::shared_ptr<const PredicateNode>& node);
  static const ProjectionFeatures _extract_features(const std::shared_ptr<const ProjectionNode>& node);
  static const JoinFeatures _extract_features(const std::shared_ptr<const JoinNode>& node);
  static const AggregateFeatures _extract_features(const std::shared_ptr<const AggregateNode>& node);

  static void _extract_table_scan_features_for_predicate_expression(
      const std::shared_ptr<AbstractLQPNode>& input, TableScanFeatures& features,
      const std::shared_ptr<AbstractPredicateExpression>& expression);

  //  static const ColumnFeatures _extract_features_for_column_expression(
  //          const std::shared_ptr<AbstractLQPNode>& input, const ColumnID& column_id, const std::string& prefix);
  //
  //  static const std::map<EncodingType, size_t> _get_encoding_type_for_column(const LQPColumnReference& reference);
  //  static size_t _get_memory_usage_for_column(const std::shared_ptr<const Table>& table, ColumnID column_id);
};

}  // namespace cost_model
}  // namespace opossum
