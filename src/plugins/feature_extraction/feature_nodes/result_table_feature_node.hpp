#pragma once

#include "abstract_feature_node.hpp"
#include "feature_extraction/feature_nodes/column_feature_node.hpp"
#include "operators/abstract_operator.hpp"
#include "feature_extraction/feature_nodes/abstract_table_feature_node.hpp"

namespace opossum {

class ResultTableFeatureNode : public AbstractTableFeatureNode {
 public:
  ResultTableFeatureNode(const TableType table_type, const uint64_t row_count, const uint64_t chunk_count,
                   const uint16_t column_count, const std::shared_ptr<const AbstractOperator>& op);

  static std::shared_ptr<ResultTableFeatureNode> from_operator(const std::shared_ptr<const AbstractOperator>& op);

  void set_column_materialized(const ColumnID column_id);

  void set_column_reference(const ColumnID column_id);

  bool column_is_materialized(const ColumnID column_id) const;

  bool column_is_reference(const ColumnID column_id) const;

  const std::unordered_set<ColumnID>& materialized_columns() const;

  const AbstractOperatorPerformanceData& performance_data() const;


 protected:
  size_t _on_shallow_hash() const final;

  std::unordered_set<ColumnID> _materialized_columns{};

  std::unordered_set<ColumnID> _reference_columns{};

  std::shared_ptr<const AbstractOperator> _op;
};

}  // namespace opossum
