#include "result_table_feature_node.hpp"


namespace opossum {

ResultTableFeatureNode::ResultTableFeatureNode(const TableType table_type, const uint64_t row_count, const uint64_t chunk_count,
                                   const uint16_t column_count, const std::shared_ptr<const AbstractOperator>& op)
    : AbstractTableFeatureNode{AbstractTableFeatureNode::TableNodeType::ResultTable, table_type, row_count, chunk_count, column_count}, _op{op} {
        if (table_type == TableType::Data) {
          for (auto column_id = ColumnID{0}; column_id < column_count; ++column_id) {
            set_column_materialized(column_id);
          }
        }
      }

std::shared_ptr<ResultTableFeatureNode> ResultTableFeatureNode::from_operator(
    const std::shared_ptr<const AbstractOperator>& op) {
  const auto& performance_data = *op->performance_data;
  return std::make_shared<ResultTableFeatureNode>(performance_data.output_table_type, performance_data.output_row_count,
                                            performance_data.output_chunk_count, performance_data.output_column_count,
                                            op);
}

size_t ResultTableFeatureNode::_on_shallow_hash() const {
  return boost::hash_value(_table_type);
}

  void ResultTableFeatureNode::set_column_materialized(const ColumnID column_id) {
    _materialized_columns.emplace(column_id);
  }

  bool ResultTableFeatureNode::column_is_materialized(const ColumnID column_id) const {
    return !_reference_columns.contains(column_id) || _materialized_columns.contains(column_id);
  }

  bool ResultTableFeatureNode::column_is_reference(const ColumnID column_id) const {
    return _reference_columns.contains(column_id) && !_materialized_columns.contains(column_id);
  }

  const std::unordered_set<ColumnID>& ResultTableFeatureNode::materialized_columns() const {
    return _materialized_columns;
  }

  const AbstractOperatorPerformanceData& ResultTableFeatureNode::performance_data() const {
    return *_op->performance_data;
  }

  void ResultTableFeatureNode::set_column_reference(const ColumnID column_id) {
    _reference_columns.emplace(column_id);
  }

}  // namespace opossum
