#pragma once

#include <atomic>
#include <mutex>
#include <string>

#include "expression/lqp_column_expression.hpp"
#include "operators/abstract_aggregate_operator.hpp"
#include "operators/abstract_join_operator.hpp"
#include "operators/abstract_operator.hpp"
#include "operators/get_table.hpp"
#include "operators/table_scan.hpp"
#include "storage/table.hpp"

namespace opossum {

class OperatorFeatureExporter {
 public:
  struct GeneralOperatorInformation {
    GeneralOperatorInformation() = default;
    pmr_string name;
    int64_t left_input_rows{0};
    int64_t right_input_rows{0};
    int32_t left_input_columns{0};
    int32_t right_input_columns{0};
    int64_t walltime{0};
    int64_t output_rows{0};
    int32_t output_columns{0};
  };

  struct TableColumnInformation {
    TableColumnInformation(const pmr_string& init_table_name, const pmr_string& init_column_name,
                           const pmr_string& init_column_type)
        : table_name(init_table_name), column_name(init_column_name), column_type(init_column_type) {}
    pmr_string table_name;
    pmr_string column_name;
    pmr_string column_type;
  };

  enum class InputSide { Left, Right };

  explicit OperatorFeatureExporter(const std::string& path_to_dir);

  void export_to_csv(const std::shared_ptr<const AbstractOperator> op);

  void flush();

 protected:
  void _export_operator(const std::shared_ptr<const AbstractOperator>& op);
  void _export_general_operator(const std::shared_ptr<const AbstractOperator>& op);
  void _export_aggregate(const std::shared_ptr<const AbstractAggregateOperator>& op);
  void _export_join(const std::shared_ptr<const AbstractJoinOperator>& op);
  void _export_get_table(const std::shared_ptr<const GetTable>& op);
  void _export_table_scan(const std::shared_ptr<const TableScan>& op);

  void _export_join_stages(const std::shared_ptr<const AbstractJoinOperator>& op);

  const GeneralOperatorInformation _general_operator_information(
      const std::shared_ptr<const AbstractOperator>& op) const;

  const TableColumnInformation _table_column_information(
      const std::shared_ptr<const AbstractLQPNode>& lqp_node,
      const std::shared_ptr<const LQPColumnExpression>& column_expression,
      const InputSide input_side = InputSide::Left) const;

  const std::shared_ptr<Table> _general_output_table =
      std::make_shared<Table>(TableColumnDefinitions{{"OPERATOR_NAME", DataType::String, false},
                                                     {"INPUT_ROWS", DataType::Long, false},
                                                     {"INPUT_COLUMNS", DataType::Int, false},
                                                     {"OUTPUT_ROWS", DataType::Long, false},
                                                     {"OUTPUT_COLUMNS", DataType::Int, false},
                                                     {"RUNTIME_NS", DataType::Long, false},
                                                     {"COLUMN_TYPE", DataType::String, false},
                                                     {"TABLE_NAME", DataType::String, false},
                                                     {"COLUMN_NAME", DataType::String, false},
                                                     {"OPERATOR_IMPLEMENTATION", DataType::String, true}},
                              TableType::Data);
  const std::shared_ptr<Table> _join_output_table =
      std::make_shared<Table>(TableColumnDefinitions{{"JOIN_ID", DataType::Int, false},
                                                     {"JOIN_IMPLEMENTATION", DataType::String, false},
                                                     {"JOIN_MODE", DataType::String, false},
                                                     {"INPUT_ROWS_LEFT", DataType::Long, false},
                                                     {"INPUT_ROWS_RIGHT", DataType::Long, false},
                                                     {"INPUT_COLUMNS_LEFT", DataType::Int, false},
                                                     {"INPUT_COLUMNS_RIGHT", DataType::Int, false},
                                                     {"OUTPUT_ROWS", DataType::Long, false},
                                                     {"OUTPUT_COLUMNS", DataType::Int, false},
                                                     {"RUNTIME_NS", DataType::Long, false},
                                                     {"LEFT_TABLE_NAME", DataType::String, false},
                                                     {"LEFT_COLUMN_NAME", DataType::String, false},
                                                     {"LEFT_COLUMN_TYPE", DataType::String, false},
                                                     {"RIGHT_TABLE_NAME", DataType::String, false},
                                                     {"RIGHT_COLUMN_NAME", DataType::String, false},
                                                     {"RIGHT_COLUMN_TYPE", DataType::String, false}},
                              TableType::Data);
  const std::shared_ptr<Table> _join_stages_table =
      std::make_shared<Table>(TableColumnDefinitions{{"JOIN_ID", DataType::Int, false},
                                                     {"STAGE_NAME", DataType::String, false},
                                                     {"RUNTIME_NS", DataType::Long, false}},
                              TableType::Data);

  const std::string _path_to_dir;
  const std::string _output_path;
  const std::string _join_output_path;
  const std::string _join_stages_output_path;

  int32_t _current_join_id{0};
  mutable std::mutex _mutex;
};
}  // namespace opossum
