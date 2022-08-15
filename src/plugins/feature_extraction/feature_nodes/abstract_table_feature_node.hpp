#pragma once

#include "feature_extraction/feature_nodes/abstract_feature_node.hpp"
#include "feature_extraction/feature_nodes/column_feature_node.hpp"

namespace hyrise {

class AbstractTableFeatureNode : public AbstractFeatureNode {
 public:
  enum class TableNodeType { BaseTable, ResultTable };

  AbstractTableFeatureNode(const TableNodeType node_type, const TableType table_type, const uint64_t row_count,
                           const uint32_t chunk_count, const uint16_t column_count);

  const std::vector<std::string>& feature_headers() const final;

  static const std::vector<std::string>& headers();

  bool is_base_table() const;

  bool registered_column(ColumnID column_id) const;

  std::shared_ptr<ColumnFeatureNode> get_column(ColumnID column_id) const;

  void register_column(const std::shared_ptr<ColumnFeatureNode>& column);

  virtual bool column_is_nullable(const ColumnID column_id) const = 0;
  virtual DataType column_data_type(const ColumnID column_id) const = 0;
  virtual uint32_t sorted_segment_count(const ColumnID column_id) const = 0;
  virtual uint32_t chunk_count() const = 0;

 protected:
  std::shared_ptr<FeatureVector> _on_to_feature_vector() const final;
  //size_t _on_shallow_hash() const final;

  TableNodeType _node_type;
  TableType _table_type;
  uint64_t _row_count;
  uint32_t _chunk_count;
  uint16_t _column_count;
  std::vector<std::weak_ptr<ColumnFeatureNode>> _columns;
};

}  // namespace hyrise
