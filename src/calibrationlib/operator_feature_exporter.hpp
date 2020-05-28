#pragma once

#include <string>

#include "operators/abstract_operator.hpp"
#include "storage/table.hpp"

namespace opossum {

class OperatorFeatureExporter {
 public:
  explicit OperatorFeatureExporter(const std::string& path_to_dir);

  void export_to_csv(const std::shared_ptr<const AbstractOperator> op);

  void flush();

 protected:
  void _export_operator(const std::shared_ptr<const AbstractOperator>& op);

  void _add_aggregate_details(const std::shared_ptr<const AbstractOperator>& op);

  void _add_join_details(const std::shared_ptr<const AbstractOperator>& op);

  void _add_table_scan_details(const std::shared_ptr<const AbstractOperator>& op,
                               const std::shared_ptr<const AbstractLQPNode>& lqp_node,
                               const std::shared_ptr<const AbstractLQPNode>& original_node);

  const std::shared_ptr<Table> _output_table =
      std::make_shared<Table>(TableColumnDefinitions{{"OPERATOR_NAME", DataType::String, false},
                                                     {"INPUT_ROWS_LEFT", DataType::Long, false},
                                                     {"INPUT_ROWS_RIGHT", DataType::Long, false},
                                                     {"OUTPUT_ROWS", DataType::Long, false},
                                                     {"RUNTIME_NS", DataType::Long, false},
                                                     {"OPERATOR_DETAIL", DataType::String, true},
                                                     {"TABLE_NAME", DataType::String, true},
                                                     {"COLUMN_NAME", DataType::String, true},
                                                     {"OPERATOR_IMPLEMENTATION", DataType::String, true}},
                              TableType::Data);

  const std::string _path_to_dir;
  std::vector<AllTypeVariant> _current_row;
};
}  // namespace opossum
