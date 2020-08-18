#include "operator_feature_exporter.hpp"

#include <boost/algorithm/string.hpp>
#include <magic_enum.hpp>

#include "expression/expression_utils.hpp"
#include "expression/lqp_column_expression.hpp"
#include "hyrise.hpp"
#include "import_export/csv/csv_writer.hpp"
#include "logical_query_plan/stored_table_node.hpp"
#include "operators/abstract_join_operator.hpp"
#include "operators/get_table.hpp"
#include "operators/join_hash.hpp"
#include "operators/pqp_utils.hpp"
#include "utils/assert.hpp"

namespace opossum {

OperatorFeatureExporter::OperatorFeatureExporter(const std::string& path_to_dir)
    : _path_to_dir(path_to_dir),
      _output_path(path_to_dir + "/operators.csv"),
      _join_output_path(_path_to_dir + "/joins.csv"),
      _join_stages_output_path(path_to_dir + "/join_stages.csv") {}

void OperatorFeatureExporter::export_to_csv(const std::shared_ptr<const AbstractOperator> op) {
  std::lock_guard<std::mutex> lock(_mutex);
  visit_pqp(op, [&](const auto& node) {
    Assert(op->performance_data->has_output, "Expected operator to have been executed.");
    _export_operator(node);
    return PQPVisitation::VisitInputs;
  });
}

void OperatorFeatureExporter::flush() {
  std::lock_guard<std::mutex> lock(_mutex);
  CsvWriter::write(*_general_output_table, _output_path);
  CsvWriter::write(*_join_output_table, _join_output_path);
  CsvWriter::write(*_join_stages_table, _join_stages_output_path);
}

const OperatorFeatureExporter::GeneralOperatorInformation OperatorFeatureExporter::_general_operator_information(
    const std::shared_ptr<const AbstractOperator>& op) {
  GeneralOperatorInformation operator_info;
  operator_info.name = pmr_string{op->name()};

  if (op->left_input()) {
    operator_info.left_input_rows = static_cast<int64_t>(op->left_input()->performance_data->output_row_count);
    operator_info.left_input_columns = static_cast<int32_t>(op->left_input()->performance_data->output_column_count);
  }
  if (op->right_input()) {
    operator_info.right_input_rows = static_cast<int64_t>(op->right_input()->performance_data->output_row_count);
    operator_info.right_input_columns = static_cast<int32_t>(op->right_input()->performance_data->output_column_count);
  }

  operator_info.output_rows = static_cast<int64_t>(op->performance_data->output_row_count);
  operator_info.walltime = static_cast<int64_t>(op->performance_data->walltime.count());
  operator_info.output_columns = static_cast<int32_t>(op->performance_data->output_column_count);

  return operator_info;
}

void OperatorFeatureExporter::_export_operator(const std::shared_ptr<const AbstractOperator>& op) {
  switch (op->type()) {
    case OperatorType::Aggregate:
      _export_aggregate(static_pointer_cast<const AbstractAggregateOperator>(op));
      break;
    case OperatorType::GetTable:
      _export_get_table(static_pointer_cast<const GetTable>(op));
      break;
    case OperatorType::JoinHash:
    case OperatorType::JoinSortMerge:
    case OperatorType::JoinNestedLoop:
      _export_join(static_pointer_cast<const AbstractJoinOperator>(op));
      break;
    case OperatorType::TableScan:
      _export_table_scan(static_pointer_cast<const TableScan>(op));
      break;
    default:
      _export_general_operator(op);
  }
}

void OperatorFeatureExporter::_export_general_operator(const std::shared_ptr<const AbstractOperator>& op) {
  const auto& operator_info = _general_operator_information(op);

  auto output_row = std::vector<AllTypeVariant>{operator_info.name,
                                                operator_info.left_input_rows,
                                                operator_info.left_input_columns,
                                                operator_info.output_rows,
                                                operator_info.output_columns,
                                                operator_info.walltime,
                                                NULL_VALUE,
                                                NULL_VALUE,
                                                NULL_VALUE,
                                                NULL_VALUE};

  const auto node = op->lqp_node;
  for (const auto& el : node->node_expressions) {
    visit_expression(el, [&](const auto& expression) {
      if (expression->type == ExpressionType::LQPColumn) {
        const auto column_expression = std::dynamic_pointer_cast<LQPColumnExpression>(expression);

        if (const auto& original_node = column_expression->original_node.lock()) {
          if (original_node->type == LQPNodeType::StoredTable) {
            const auto stored_table_node = std::dynamic_pointer_cast<const StoredTableNode>(original_node);
            const auto table_name = stored_table_node->table_name;
            output_row[7] = pmr_string{table_name};
            const auto original_column_id = column_expression->original_column_id;
            const auto table = Hyrise::get().storage_manager.get_table(table_name);
            const auto column_name = pmr_string{table->column_names()[original_column_id]};
            output_row[8] = pmr_string{column_name};
          }
        }
      }
      return ExpressionVisitation::VisitArguments;
    });
  }

  _general_output_table->append(output_row);
}

void OperatorFeatureExporter::_export_aggregate(const std::shared_ptr<const AbstractAggregateOperator>& op) {
  const auto& operator_info = _general_operator_information(op);

  auto output_row = std::vector<AllTypeVariant>{pmr_string{"Aggregate"},
                                                operator_info.left_input_rows,
                                                operator_info.left_input_columns,
                                                operator_info.output_rows,
                                                operator_info.output_columns,
                                                operator_info.walltime,
                                                NULL_VALUE,
                                                NULL_VALUE,
                                                NULL_VALUE,
                                                operator_info.name};

  _general_output_table->append(output_row);
}

void OperatorFeatureExporter::_export_join(const std::shared_ptr<const AbstractJoinOperator>& op) {
  const auto& operator_info = _general_operator_information(op);
  const auto join_mode = pmr_string{join_mode_to_string.left.at(op->mode())};
  _export_join_stages(op);

  const auto output_row = std::vector<AllTypeVariant>{_current_join_id,
                                                      operator_info.name,
                                                      join_mode,
                                                      operator_info.left_input_rows,
                                                      operator_info.right_input_rows,
                                                      operator_info.left_input_columns,
                                                      operator_info.right_input_columns,
                                                      operator_info.output_rows,
                                                      operator_info.output_columns,
                                                      operator_info.walltime,
                                                      NULL_VALUE,
                                                      NULL_VALUE,
                                                      NULL_VALUE,
                                                      NULL_VALUE};

  _join_output_table->append(output_row);
  ++_current_join_id;
}

void OperatorFeatureExporter::_export_get_table(const std::shared_ptr<const GetTable>& op) {
  const auto& operator_info = _general_operator_information(op);

  const auto output_row = std::vector<AllTypeVariant>{operator_info.name,
                                                      operator_info.left_input_rows,
                                                      operator_info.left_input_columns,
                                                      operator_info.output_rows,
                                                      operator_info.output_columns,
                                                      operator_info.walltime,
                                                      NULL_VALUE,
                                                      pmr_string{op->table_name()},
                                                      NULL_VALUE,
                                                      NULL_VALUE};

  _general_output_table->append(output_row);
}

void OperatorFeatureExporter::_export_table_scan(const std::shared_ptr<const TableScan>& op) {
  const auto& operator_info = _general_operator_information(op);

  pmr_string scan_type = "REFERENCE_SCAN";
  const auto lqp_node = op->lqp_node;
  if (lqp_node->left_input() && lqp_node->left_input()->type == LQPNodeType::StoredTable) {
    scan_type = "COLUMN_SCAN";
  }

  Assert(op->_impl_description != "Unset", "Expected TableScan to be executed.");
  const auto implementation = pmr_string{op->_impl_description};

  const auto output_row = std::vector<AllTypeVariant>{operator_info.name,
                                                      operator_info.left_input_rows,
                                                      operator_info.left_input_columns,
                                                      operator_info.output_rows,
                                                      operator_info.output_columns,
                                                      operator_info.walltime,
                                                      scan_type,
                                                      NULL_VALUE,
                                                      NULL_VALUE,
                                                      implementation};
  _general_output_table->append(output_row);
}

void OperatorFeatureExporter::_export_join_stages(const std::shared_ptr<const AbstractJoinOperator>& op) {
  if (const auto join_operator = std::dynamic_pointer_cast<const JoinHash>(op)) {
    const auto& performance_data =
        dynamic_cast<OperatorPerformanceData<JoinHash::OperatorSteps>&>(*(join_operator->performance_data));
    constexpr auto steps = magic_enum::enum_entries<JoinHash::OperatorSteps>();

    for (const auto& step : steps) {
      const auto runtime = static_cast<int64_t>(performance_data.get_step_runtime(step.first).count());
      _join_stages_table->append({static_cast<int32_t>(_current_join_id), pmr_string{step.second}, runtime});
    }
  }
}

}  // namespace opossum
