#pragma once

#include <string>

#include "expression/abstract_predicate_expression.hpp"
#include "expression/pqp_column_expression.hpp"

#include "cost_estimation/feature/column_features.hpp"
#include "expression/lqp_column_expression.hpp"
#include "logical_query_plan/abstract_lqp_node.hpp"
#include "logical_query_plan/aggregate_node.hpp"
#include "logical_query_plan/join_node.hpp"
#include "logical_query_plan/predicate_node.hpp"
#include "logical_query_plan/projection_node.hpp"

#include "storage/abstract_segment.hpp"
#include "storage/encoding_type.hpp"

namespace opossum {
namespace cost_model {

class ColumnFeatureExtractor {
 public:
  static const ColumnFeatures extract_features(const std::shared_ptr<AbstractLQPNode>& node,
                                               const std::shared_ptr<LQPColumnExpression>& column_expression,
                                               const std::string& prefix);

  static const ColumnFeatures extract_features(const std::shared_ptr<const Table>& table, const ColumnID column_id,
                                               const DataType data_type, const std::string& prefix);

 private:
  static std::pair<SegmentEncodingSpec, bool> _get_encoding_type_for_segment(
      const std::shared_ptr<AbstractSegment>& segment);
  static size_t _get_memory_usage_for_column(const std::shared_ptr<const Table>& table, const ColumnID column_id);
  //            static const ColumnFeatures _extract_features_for_column_expression(
  //                    const std::shared_ptr<AbstractLQPNode>& input, const ColumnID& column_id, const std::string& prefix);
  //
  //            static const std::map<EncodingType, size_t> _get_encoding_type_for_column(const LQPColumnReference& reference);
  //            static size_t _get_memory_usage_for_column(const std::shared_ptr<const Table>& table, ColumnID column_id);
};

}  // namespace cost_model
}  // namespace opossum
