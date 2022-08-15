#include "abstract_table_feature_node.hpp"

#include "feature_extraction/util/feature_extraction_utils.hpp"

namespace hyrise {

AbstractTableFeatureNode::AbstractTableFeatureNode(const TableNodeType node_type, const TableType table_type,
                                                   const uint64_t row_count, const uint32_t chunk_count,
                                                   const uint16_t column_count)
    : AbstractFeatureNode{FeatureNodeType::Table, nullptr},
      _node_type{node_type},
      _table_type{table_type},
      _row_count{row_count},
      _chunk_count{chunk_count},
      _column_count{column_count} {
  _columns.resize(column_count);
}

std::shared_ptr<FeatureVector> AbstractTableFeatureNode::_on_to_feature_vector() const {
  auto feature_vector = one_hot_encoding<TableType>(_table_type);
  feature_vector->reserve(feature_vector->size() + 3);
  feature_vector->emplace_back(static_cast<Feature::base_type>(_row_count));
  feature_vector->emplace_back(static_cast<Feature>(_chunk_count));
  feature_vector->emplace_back(static_cast<Feature>(_column_count));
  return feature_vector;
}

const std::vector<std::string>& AbstractTableFeatureNode::feature_headers() const {
  return headers();
}

const std::vector<std::string>& AbstractTableFeatureNode::headers() {
  static auto ohe_headers_type = one_hot_headers<TableType>("table_type.");
  static const auto headers = std::vector<std::string>{"row_count", "chunk_count", "column_count"};
  if (ohe_headers_type.size() == magic_enum::enum_count<TableType>()) {
    ohe_headers_type.insert(ohe_headers_type.end(), headers.begin(), headers.end());
  }
  return ohe_headers_type;
}

bool AbstractTableFeatureNode::is_base_table() const {
  return _node_type == TableNodeType::BaseTable;
}

bool AbstractTableFeatureNode::registered_column(ColumnID column_id) const {
  Assert(_columns.size() > column_id, "Invalid column ID");
  return !_columns[column_id].expired();
}

std::shared_ptr<ColumnFeatureNode> AbstractTableFeatureNode::get_column(ColumnID column_id) const {
  Assert(_columns.size() > column_id, "Invalid column ID");
  Assert(!_columns[column_id].expired(), "Column was not set");
  return _columns[column_id].lock();
}

void AbstractTableFeatureNode::register_column(const std::shared_ptr<ColumnFeatureNode>& column) {
  const auto column_id = column->column_id();
  Assert(_columns.size() > column_id, "Invalid column ID");
  Assert(_columns[column_id].expired(), "Column was already set");
  _columns[column_id] = column;
}

}  // namespace hyrise
