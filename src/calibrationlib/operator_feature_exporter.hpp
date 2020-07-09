#pragma once

#include <mutex>
#include <string>

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

  explicit OperatorFeatureExporter(const std::string& path_to_dir);

  void export_to_csv(const std::shared_ptr<const AbstractOperator> op);

  void flush();

 protected:
  void _export_operator(const std::shared_ptr<const AbstractOperator>& op);
  void _export_operator(const std::shared_ptr<const AbstractJoinOperator>& op);
  void _export_operator(const std::shared_ptr<const AbstractAggregateOperator>& op);
  void _export_operator(const std::shared_ptr<const GetTable>& op);
  void _export_operator(const std::shared_ptr<const TableScan>& op);

  std::shared_ptr<const GeneralOperatorInformation> _general_operator_information(
      const std::shared_ptr<const AbstractOperator>& op);

  // currently, supports only HashJoin stages
  void _export_join_stages(const std::shared_ptr<const AbstractJoinOperator>& op);

  const std::shared_ptr<Table> _general_output_table =
      std::make_shared<Table>(TableColumnDefinitions{{"OPERATOR_NAME", DataType::String, false},
                                                     {"INPUT_ROWS", DataType::Long, false},
                                                     {"INPUT_COLUMNS", DataType::Int, false},
                                                     {"OUTPUT_ROWS", DataType::Long, false},
                                                     {"OUTPUT_COLUMNS", DataType::Int, false},
                                                     {"RUNTIME_NS", DataType::Long, false},
                                                     {"OPERATOR_DETAIL", DataType::String, true},
                                                     {"TABLE_NAME", DataType::String, true},
                                                     {"COLUMN_NAME", DataType::String, true},
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
                                                     {"OUTPUT_COLUMNS", DataType::Long, false},
                                                     {"RUNTIME_NS", DataType::Long, false},
                                                     {"LEFT_TABLE_NAME", DataType::String, true},
                                                     {"RIGHT_TABLE_NAME", DataType::String, true},
                                                     {"LEFT_COLUMN_NAME", DataType::String, true},
                                                     {"RIGHT_COLUMN_NAME", DataType::String, true}},
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

  size_t _current_join_id{0};
  mutable std::mutex _mutex;
};
}  // namespace opossum
