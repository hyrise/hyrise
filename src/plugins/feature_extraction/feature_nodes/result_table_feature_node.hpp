#pragma once

#include "feature_extraction/feature_nodes/abstract_table_feature_node.hpp"
#include "feature_extraction/feature_nodes/column_feature_node.hpp"
#include "operators/abstract_operator.hpp"

namespace opossum {

class ResultTableFeatureNode : public AbstractTableFeatureNode {
 public:
  ResultTableFeatureNode(const TableType table_type, const uint64_t row_count, const uint32_t chunk_count,
                         const uint16_t column_count, const std::shared_ptr<const AbstractOperator>& op);

  static std::shared_ptr<ResultTableFeatureNode> from_operator(const std::shared_ptr<const AbstractOperator>& op);

  void set_column_materialized(const ColumnID column_id);

  bool column_is_materialized(const ColumnID column_id) const;

  const std::unordered_set<ColumnID>& materialized_columns() const;

  const AbstractOperatorPerformanceData& performance_data() const;

  TableType table_type() const;

  bool column_is_nullable(const ColumnID column_id) const final;
  DataType column_data_type(const ColumnID column_id) const final;
  uint32_t sorted_segment_count(const ColumnID column_id) const final;
  uint32_t chunk_count() const final;

  bool column_is_reference(const ColumnID column_id) const;

 protected:
  size_t _on_shallow_hash() const final;

  std::unordered_set<ColumnID> _materialized_columns{};

  std::shared_ptr<const AbstractOperator> _op;
};

}  // namespace opossum
