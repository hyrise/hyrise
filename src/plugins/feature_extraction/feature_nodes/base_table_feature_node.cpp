#include "base_table_feature_node.hpp"

#include "feature_extraction/util/feature_extraction_utils.hpp"

namespace opossum {

BaseTableFeatureNode::BaseTableFeatureNode(const uint64_t row_count, const uint32_t chunk_count,
                                           const uint16_t column_count, const std::shared_ptr<Table>& table,
                                           const std::string& table_name)
    : AbstractTableFeatureNode{AbstractTableFeatureNode::TableNodeType::BaseTable, TableType::Data, row_count,
                               chunk_count, column_count},
      _table{table},
      _table_name{table_name} {}

std::shared_ptr<BaseTableFeatureNode> BaseTableFeatureNode::from_table(const std::shared_ptr<Table>& table,
                                                                       const std::string& table_name) {
  return std::make_shared<BaseTableFeatureNode>(table->row_count(), table->chunk_count(), table->column_count(), table,
                                                table_name);
}

size_t BaseTableFeatureNode::_on_shallow_hash() const {
  return boost::hash_value(*_table_name);
}

const std::optional<std::string>& BaseTableFeatureNode::table_name() const {
  return _table_name;
}

std::shared_ptr<Table> BaseTableFeatureNode::table() const {
  return _table;
}

bool BaseTableFeatureNode::column_is_nullable(const ColumnID column_id) const {
  return _table->column_is_nullable(column_id);
}

DataType BaseTableFeatureNode::column_data_type(const ColumnID column_id) const {
  return _table->column_data_type(column_id);
}

uint32_t BaseTableFeatureNode::sorted_segment_count(const ColumnID column_id) const {
  auto sorted_segment_count = uint32_t{0};
  for (auto chunk_id = ChunkID{0}; chunk_id < _chunk_count; ++chunk_id) {
    const auto& chunk = _table->get_chunk(chunk_id);
    if (!chunk) {
      continue;
    }
    const auto& segments_sorted_by = chunk->individually_sorted_by();

    if (std::any_of(segments_sorted_by.cbegin(), segments_sorted_by.cend(),
                    [&](const auto& sort_definition) { return sort_definition.column == column_id; })) {
      ++sorted_segment_count;
    }
  }

  return sorted_segment_count;
}

uint32_t BaseTableFeatureNode::chunk_count() const {
  return _table->chunk_count();
}

}  // namespace opossum
