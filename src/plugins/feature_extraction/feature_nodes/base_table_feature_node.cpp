#include "base_table_feature_node.hpp"

#include "feature_extraction/util/feature_extraction_utils.hpp"

namespace opossum {


BaseTableFeatureNode::BaseTableFeatureNode(const uint64_t row_count, const uint64_t chunk_count,
                                   const uint16_t column_count, const std::shared_ptr<Table>& table,
                                   const std::string& table_name)
    : AbstractTableFeatureNode{AbstractTableFeatureNode::TableNodeType::BaseTable, TableType::Data, row_count, chunk_count, column_count},
      _table{table},
      _table_name{table_name} {}

std::shared_ptr<BaseTableFeatureNode> BaseTableFeatureNode::from_table(const std::shared_ptr<Table>& table,
                                                               const std::string& table_name) {
  return std::make_shared<BaseTableFeatureNode>(table->row_count(), table->chunk_count(),
                                            table->column_count(), table, table_name);
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

}  // namespace opossum
