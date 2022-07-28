#pragma once

#include "abstract_table_feature_node.hpp"
#include "feature_extraction/feature_nodes/column_feature_node.hpp"
#include "operators/abstract_operator.hpp"
#include "storage/table.hpp"

namespace opossum {

class BaseTableFeatureNode : public AbstractTableFeatureNode {
 public:
  BaseTableFeatureNode(const uint64_t row_count, const uint32_t chunk_count, const uint16_t column_count,
                       const std::shared_ptr<Table>& table, const std::string& table_name);

  static std::shared_ptr<BaseTableFeatureNode> from_table(const std::shared_ptr<Table>& table,
                                                          const std::string& table_name);

  bool is_base_table() const;

  const std::optional<std::string>& table_name() const;

  bool registered_column(ColumnID column_id) const;

  std::shared_ptr<ColumnFeatureNode> get_column(ColumnID column_id) const;

  void register_column(const std::shared_ptr<ColumnFeatureNode>& column);

  std::shared_ptr<Table> table() const;

  bool column_is_nullable(const ColumnID column_id) const final;
  DataType column_data_type(const ColumnID column_id) const final;
  uint32_t sorted_segment_count(const ColumnID column_id) const final;
  uint32_t chunk_count() const final;

 protected:
  size_t _on_shallow_hash() const final;

  std::shared_ptr<Table> _table;
  std::optional<std::string> _table_name;
};

}  // namespace opossum
