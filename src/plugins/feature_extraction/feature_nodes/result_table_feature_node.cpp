#include "result_table_feature_node.hpp"

namespace hyrise {

ResultTableFeatureNode::ResultTableFeatureNode(const TableType table_type, const uint64_t row_count,
                                               const uint32_t chunk_count, const uint16_t column_count,
                                               const std::shared_ptr<const AbstractOperator>& op)
    : AbstractTableFeatureNode{AbstractTableFeatureNode::TableNodeType::ResultTable, table_type, row_count, chunk_count,
                               column_count},
      _op{op},
      _output_expressions{op->lqp_node->output_expressions()} {
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
                                                  performance_data.output_chunk_count,
                                                  static_cast<uint16_t>(op->lqp_node->output_expressions().size()), op);
}

size_t ResultTableFeatureNode::_on_shallow_hash() const {
  return boost::hash_value(_table_type);
}

void ResultTableFeatureNode::set_column_materialized(const ColumnID column_id) {
  _materialized_columns.emplace(column_id);
}

bool ResultTableFeatureNode::column_is_materialized(const ColumnID column_id) const {
  return _materialized_columns.contains(column_id);
}

const std::unordered_set<ColumnID>& ResultTableFeatureNode::materialized_columns() const {
  return _materialized_columns;
}

const AbstractOperatorPerformanceData& ResultTableFeatureNode::performance_data() const {
  return *_op->performance_data;
}

TableType ResultTableFeatureNode::table_type() const {
  return _table_type;
}

bool ResultTableFeatureNode::column_is_nullable(const ColumnID column_id) const {
  return _op->lqp_node->is_column_nullable(column_id);
}

DataType ResultTableFeatureNode::column_data_type(const ColumnID column_id) const {
  return _output_expressions.at(column_id)->data_type();
}

uint32_t ResultTableFeatureNode::sorted_segment_count(const ColumnID column_id) const {
  auto sorted_segment_count = uint32_t{0};
  const auto& chunks_sorted_by = _op->performance_data->output_chunks_sorted_by;
  for (auto chunk_id = ChunkID{0}; chunk_id < _chunk_count; ++chunk_id) {
    const auto& segments_sorted_by = chunks_sorted_by.at(chunk_id);

    if (std::any_of(segments_sorted_by.cbegin(), segments_sorted_by.cend(),
                    [&](const auto& sort_definition) { return sort_definition.column == column_id; })) {
      ++sorted_segment_count;
    }
  }

  return sorted_segment_count;
}

uint32_t ResultTableFeatureNode::chunk_count() const {
  return _op->performance_data->output_chunk_count;
}

}  // namespace hyrise
