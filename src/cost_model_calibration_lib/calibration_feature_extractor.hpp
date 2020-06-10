#pragma once

#include <string>

#include "expression/abstract_predicate_expression.hpp"
#include "expression/pqp_column_expression.hpp"

#include "cost_estimation/feature/aggregate_features.hpp"
#include "cost_estimation/feature/column_features.hpp"
#include "cost_estimation/feature/constant_hardware_features.hpp"
#include "cost_estimation/feature/cost_model_features.hpp"
#include "cost_estimation/feature/join_features.hpp"
#include "cost_estimation/feature/projection_features.hpp"
#include "cost_estimation/feature/runtime_hardware_features.hpp"
#include "cost_estimation/feature/table_scan_features.hpp"

#include "operators/abstract_aggregate_operator.hpp"
#include "operators/abstract_join_operator.hpp"
#include "operators/abstract_operator.hpp"
#include "operators/get_table.hpp"
#include "operators/index_scan.hpp"
#include "operators/projection.hpp"
#include "operators/table_scan.hpp"
#include "operators/validate.hpp"

#include "storage/abstract_segment.hpp"
#include "storage/encoding_type.hpp"

namespace opossum {
namespace cost_model {

class CalibrationFeatureExtractor {
 public:
  static const std::optional<CostModelFeatures> extract_features(const std::shared_ptr<const AbstractOperator>& op);

 private:
  static const CostModelFeatures _extract_general_features(const std::shared_ptr<const AbstractOperator>& op);
  static const ConstantHardwareFeatures _extract_constant_hardware_features();
  static const RuntimeHardwareFeatures _extract_runtime_hardware_features();

  static const std::optional<TableScanFeatures> _extract_features_for_operator(const std::shared_ptr<const TableScan>& op);
  static const TableScanFeatures _extract_features_for_operator(const std::shared_ptr<const IndexScan>& op);
  static const std::optional<TableScanFeatures> _extract_features_for_operator(const std::shared_ptr<const Validate>& op);
  static const ProjectionFeatures _extract_features_for_operator(const std::shared_ptr<const Projection>& op);
  static const JoinFeatures _extract_features_for_operator(const std::shared_ptr<const AbstractJoinOperator>& op);
  static const AggregateFeatures _extract_features_for_operator(
      const std::shared_ptr<const AbstractAggregateOperator>& op);

  static void _extract_table_scan_features_for_predicate_expression(
      std::shared_ptr<const Table>& left_input_table, TableScanFeatures& features,
      const std::shared_ptr<AbstractPredicateExpression>& expression);

  static const ColumnFeatures _extract_features_for_column_expression(
      const std::shared_ptr<const Table>& left_input_table,
      const std::shared_ptr<PQPColumnExpression>& column_expression, const std::string& prefix);

  static size_t _get_memory_usage_for_column(const std::shared_ptr<const Table>& table, ColumnID column_id);
};

}  // namespace cost_model
}  // namespace opossum
