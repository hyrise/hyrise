#pragma once

#include "abstract_feature_node.hpp"
#include "feature_extraction/feature_nodes/column_feature_node.hpp"
#include "operators/abstract_operator.hpp"
#include "storage/table.hpp"

namespace opossum {

class BaseTableFeatureNode : public AbstractFeatureNode {
 public:
  TableFeatureNode(const TableType table_type, const uint64_t row_count, const uint64_t chunk_count,
                   const uint16_t column_count, const std::shared_ptr<AbstractFeatureNode>& input_node);

  TableFeatureNode(const TableType table_type, const uint64_t row_count, const uint64_t chunk_count,
                   const uint16_t column_count, const std::shared_ptr<Table>& table, const std::string& table_name);

  static std::shared_ptr<TableFeatureNode> from_performance_data(
      const AbstractOperatorPerformanceData& performance_data, const std::shared_ptr<AbstractFeatureNode>& input_node);

  static std::shared_ptr<TableFeatureNode> from_table(const std::shared_ptr<Table>& table,
                                                      const std::string& table_name);

  const std::vector<std::string>& feature_headers() const final;

  static const std::vector<std::string>& headers();

  bool is_base_table() const;

  const std::optional<std::string>& table_name() const;

  bool registered_column(ColumnID column_id) const;

  std::shared_ptr<ColumnFeatureNode> get_column(ColumnID column_id) const;

  void register_column(const std::shared_ptr<ColumnFeatureNode>& column);

  std::shared_ptr<Table> table() const;

 protected:
  std::shared_ptr<FeatureVector> _on_to_feature_vector() const final;
  size_t _on_shallow_hash() const final;

  TableType _table_type;
  uint64_t _row_count;
  uint64_t _chunk_count;
  uint16_t _column_count;
  std::shared_ptr<Table> _table;
  std::optional<std::string> _table_name;
  std::vector<std::weak_ptr<ColumnFeatureNode>> _columns;
};

}  // namespace opossum
