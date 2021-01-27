#pragma once

#include <atomic>
#include <mutex>

#include "expression/lqp_column_expression.hpp"
#include "operators/abstract_aggregate_operator.hpp"
#include "operators/abstract_join_operator.hpp"
#include "operators/abstract_operator.hpp"
#include "operators/get_table.hpp"
#include "operators/index_scan.hpp"
#include "operators/join_hash.hpp"
#include "operators/table_scan.hpp"
#include "statistics/cardinality_estimator.hpp"
#include "storage/table.hpp"
#include "storage/table_column_definition.hpp"

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
    int64_t left_input_chunks{0};
    int64_t right_input_chunks{0};
    int64_t walltime{0};
    int64_t output_rows{0};
    int32_t output_columns{0};
    float estimated_cardinality{0.0};
    float estimated_left_input_rows{0.0};
    float estimated_right_input_rows{0.0};
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
  void export_to_csv(const std::shared_ptr<const AbstractOperator> op, const std::string& query);
  void flush();

 protected:
  bool _has_column(const std::shared_ptr<const AbstractOperator>& op, const std::string& column_name) const;
  bool _data_arrives_ordered(const std::shared_ptr<const AbstractOperator>& op, const std::string& table_name, const std::string& column_name) const;
  void _export_to_csv(const std::shared_ptr<const AbstractOperator>& op);
  void _export_operator(const std::shared_ptr<const AbstractOperator>& op);
  void _export_general_operator(const std::shared_ptr<const AbstractOperator>& op);
  void _export_aggregate(const std::shared_ptr<const AbstractAggregateOperator>& op);
  void _export_join(const std::shared_ptr<const AbstractJoinOperator>& op);
  size_t _get_pruned_chunk_count(std::shared_ptr<const AbstractOperator> op, const std::string& table_name);
  void _export_get_table(const std::shared_ptr<const GetTable>& op);
  void _export_table_scan(const std::shared_ptr<const TableScan>& op);
  void _export_index_scan(const std::shared_ptr<const IndexScan>& op);

  void _export_join_stages(const std::shared_ptr<const AbstractJoinOperator>& op);

  const GeneralOperatorInformation _general_operator_information(
      const std::shared_ptr<const AbstractOperator>& op) const;

  const TableColumnInformation _table_column_information(
      const std::shared_ptr<const AbstractLQPNode>& lqp_node,
      const std::shared_ptr<const LQPColumnExpression>& column_expression,
      const InputSide input_side = InputSide::Left) const;

  const pmr_string _find_input_sorted(const std::unique_ptr<AbstractOperatorPerformanceData>& performance_data,
                                      const std::shared_ptr<AbstractExpression>& predicate) const;
  const pmr_string _check_column_sorted(const std::unique_ptr<AbstractOperatorPerformanceData>& performance_data,
                                        const ColumnID column_id) const;

  const std::shared_ptr<Table> _general_output_table =
      std::make_shared<Table>(TableColumnDefinitions{{"OPERATOR_NAME", DataType::String, false},
                                                     {"INPUT_ROWS", DataType::Long, false},
                                                     {"INPUT_COLUMNS", DataType::Int, false},
                                                     {"ESTIMATED_INPUT_ROWS", DataType::Float, false},
                                                     {"OUTPUT_ROWS", DataType::Long, false},
                                                     {"OUTPUT_COLUMNS", DataType::Int, false},
                                                     {"ESTIMATED_CARDINALITY", DataType::Float, false},
                                                     {"RUNTIME_NS", DataType::Long, false},
                                                     {"COLUMN_TYPE", DataType::String, false},
                                                     {"TABLE_NAME", DataType::String, false},
                                                     {"COLUMN_NAME", DataType::String, false},
                                                     {"OPERATOR_IMPLEMENTATION", DataType::String, true},
                                                     {"INPUT_COLUMN_SORTED", DataType::String, true},
                                                     {"QUERY_HASH", DataType::String, true},
                                                     {"INPUT_CHUNKS", DataType::Long, false},
                                                     {"PREDICATE", DataType::String, false},
                                                     {"SKIPPED_CHUNKS", DataType::Long, false}},
                              TableType::Data);

  const std::shared_ptr<Table> _scan_output_table =
      std::make_shared<Table>(TableColumnDefinitions{{"OPERATOR_NAME", DataType::String, false},
                                                     {"INPUT_ROWS", DataType::Long, false},
                                                     {"INPUT_COLUMNS", DataType::Int, false},
                                                     {"ESTIMATED_INPUT_ROWS", DataType::Float, false},
                                                     {"OUTPUT_ROWS", DataType::Long, false},
                                                     {"OUTPUT_COLUMNS", DataType::Int, false},
                                                     {"ESTIMATED_CARDINALITY", DataType::Float, false},
                                                     {"RUNTIME_NS", DataType::Long, false},
                                                     {"COLUMN_TYPE", DataType::String, false},
                                                     {"TABLE_NAME", DataType::String, false},
                                                     {"COLUMN_NAME", DataType::String, false},
                                                     {"OPERATOR_IMPLEMENTATION", DataType::String, true},
                                                     {"INPUT_COLUMN_SORTED", DataType::String, true},
                                                     {"QUERY_HASH", DataType::String, true},
                                                     {"INPUT_CHUNKS", DataType::Long, false},
                                                     {"PREDICATE", DataType::String, true},
                                                     {"SHORTCUT_NONE_MATCH", DataType::Long, false},
                                                     {"SHORTCUT_ALL_MATCH", DataType::Long, false},
                                                     {"SCANS_SORTED", DataType::Long, false},
                                                     {"SEGMENTS_SCANNED", DataType::Long, false}},
                              TableType::Data);

  const std::shared_ptr<Table> _join_output_table =
      std::make_shared<Table>(TableColumnDefinitions{{"JOIN_ID", DataType::Int, false},
                                                     {"JOIN_IMPLEMENTATION", DataType::String, false},
                                                     {"JOIN_MODE", DataType::String, false},
                                                     {"INPUT_ROWS_LEFT", DataType::Long, false},
                                                     {"INPUT_ROWS_RIGHT", DataType::Long, false},
                                                     {"INPUT_COLUMNS_LEFT", DataType::Int, false},
                                                     {"INPUT_COLUMNS_RIGHT", DataType::Int, false},
                                                     {"ESTIMATED_INPUT_ROWS_LEFT", DataType::Float, false},
                                                     {"ESTIMATED_INPUT_ROWS_RIGHT", DataType::Float, false},
                                                     {"ESTIMATED_LEFT_DISTINCT_VALUES", DataType::Long, false},
                                                     {"ESTIMATED_RIGHT_DISTINCT_VALUES", DataType::Long, false},
                                                     {"OUTPUT_ROWS", DataType::Long, false},
                                                     {"OUTPUT_COLUMNS", DataType::Int, false},
                                                     {"ESTIMATED_CARDINALITY", DataType::Float, false},
                                                     {"RUNTIME_NS", DataType::Long, false},
                                                     {"LEFT_TABLE_NAME", DataType::String, false},
                                                     {"LEFT_COLUMN_NAME", DataType::String, false},
                                                     {"LEFT_COLUMN_TYPE", DataType::String, false},
                                                     {"RIGHT_TABLE_NAME", DataType::String, false},
                                                     {"RIGHT_COLUMN_NAME", DataType::String, false},
                                                     {"RIGHT_COLUMN_TYPE", DataType::String, false},
                                                     {"PROBE_SIDE_FLIP", DataType::Int, false},
                                                     {"LEFT_INPUT_COLUMN_SORTED", DataType::String, true},
                                                     {"RIGHT_INPUT_COLUMN_SORTED", DataType::String, true},
                                                     {"QUERY_HASH", DataType::String, true},
                                                     {"LEFT_INPUT_CHUNKS", DataType::Long, false},
                                                     {"RIGHT_INPUT_CHUNKS", DataType::Long, false},
                                                     {"LEFT_TABLE_PRUNED_CHUNKS", DataType::Long, false},
                                                     {"RIGHT_TABLE_PRUNED_CHUNKS", DataType::Long, false},
                                                     {"LEFT_TABLE_ROW_COUNT", DataType::Long, false},
                                                     {"RIGHT_TABLE_ROW_COUNT", DataType::Long, false}},
                              TableType::Data);
  const std::shared_ptr<Table> _join_stages_table =
      std::make_shared<Table>(TableColumnDefinitions{{"JOIN_ID", DataType::Int, false},
                                                     {"STAGE_NAME", DataType::String, false},
                                                     {"RUNTIME_NS", DataType::Long, false}},
                              TableType::Data);

  const std::shared_ptr<Table> _query_table = std::make_shared<Table>(
      TableColumnDefinitions{{"HASH_VALUE", DataType::String, false}, {"SQL_STRING", DataType::String, false}},
      TableType::Data);

  const std::string _path_to_dir;
  const std::string _output_path;
  const std::string _scan_output_path;
  const std::string _join_output_path;
  const std::string _join_stages_output_path;
  const std::string _query_output_path;
  pmr_string _current_query_hash{""};
  std::shared_ptr<CardinalityEstimator> _cardinality_estimator = std::make_shared<CardinalityEstimator>();

  int32_t _current_join_id{0};
  mutable std::mutex _mutex;
};
}  // namespace opossum
