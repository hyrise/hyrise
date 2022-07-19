#pragma once

#include "abstract_feature_node.hpp"
#include "operators/abstract_operator.hpp"
#include "storage/table.hpp"

namespace opossum {

class TableFeatureNode : public AbstractFeatureNode {
 public:

  TableFeatureNode(const TableType table_type, const uint64_t row_count, const uint64_t chunk_count, const std::shared_ptr<AbstractFeatureNode>& input_node);

  TableFeatureNode(const TableType table_type, const uint64_t row_count, const uint64_t chunk_count, const std::string& table_name);

  static std::shared_ptr<TableFeatureNode> from_performance_data(const AbstractOperatorPerformanceData& performance_data, const std::shared_ptr<AbstractFeatureNode>& input_node);

  static std::shared_ptr<TableFeatureNode> from_table(const std::shared_ptr<Table>& table, const std::string& table_name);

  size_t hash() const final;

  const std::vector<std::string>& feature_headers() const final;

  static const std::vector<std::string>& headers();

  const std::optional<std::string>& table_name() const;

 protected:
  std::shared_ptr<FeatureVector>  _on_to_feature_vector() final;

  TableType _table_type;
  uint64_t _row_count;
  uint64_t _chunk_count;
  uint16_t _column_count;
  std::optional<std::string> _table_name;
};

}  // namespace opossum
