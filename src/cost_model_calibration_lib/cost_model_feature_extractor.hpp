#pragma once

#include <string>

#include "expression/abstract_predicate_expression.hpp"
#include "expression/pqp_column_expression.hpp"

#include "feature/aggregate_features.hpp"
#include "feature/calibration_features.hpp"
#include "feature/column_features.hpp"
#include "feature/constant_hardware_features.hpp"
#include "feature/runtime_hardware_features.hpp"
#include "feature/join_features.hpp"
#include "feature/projection_features.hpp"
#include "feature/table_scan_features.hpp"

#include "operators/abstract_join_operator.hpp"
#include "operators/abstract_operator.hpp"
#include "operators/aggregate.hpp"
#include "operators/get_table.hpp"
#include "operators/index_scan.hpp"
#include "operators/projection.hpp"
#include "operators/table_scan.hpp"

#include "storage/base_segment.hpp"
#include "storage/encoding_type.hpp"

namespace opossum {
namespace cost_model {

class CostModelFeatureExtractor {
 public:
  static const CalibrationFeatures extract_features(const std::shared_ptr<const AbstractOperator>& op);

 private:
  static const CalibrationFeatures _extract_general_features(const std::shared_ptr<const AbstractOperator>& op);
  static const ConstantHardwareFeatures _extract_constant_hardware_features();
  static const RuntimeHardwareFeatures _extract_runtime_hardware_features();

  static const TableScanFeatures _extract_features_for_operator(const std::shared_ptr<const TableScan>& op);
  static const TableScanFeatures _extract_features_for_operator(const std::shared_ptr<const IndexScan>& op);
  static const ProjectionFeatures _extract_features_for_operator(const std::shared_ptr<const Projection>& op);
  static const JoinFeatures _extract_features_for_operator(const std::shared_ptr<const AbstractJoinOperator>& op);
  static const AggregateFeatures _extract_features_for_operator(const std::shared_ptr<const Aggregate>& op);

  static void _extract_table_scan_features_for_predicate_expression(
      std::shared_ptr<const Table>& left_input_table, TableScanFeatures& features,
      const std::shared_ptr<AbstractPredicateExpression>& expression);

  static const ColumnFeatures _extract_features_for_column_expression(const std::shared_ptr<const Table>& left_input_table,
                                                      const std::shared_ptr<PQPColumnExpression> column_expression,
                                                      const std::string& prefix);

  static std::pair<EncodingType, bool> _get_encoding_type_for_segment(const std::shared_ptr<BaseSegment>& segment);
  static size_t _get_memory_usage_for_column(const std::shared_ptr<const Table>& table, ColumnID column_id);
};

}  // namespace cost_model
}  // namespace opossum
