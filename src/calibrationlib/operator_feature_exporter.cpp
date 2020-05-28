#include "operator_feature_exporter.hpp"

#include <boost/algorithm/string.hpp>

#include "expression/expression_utils.hpp"
#include "expression/lqp_column_expression.hpp"
#include "hyrise.hpp"
#include "import_export/csv/csv_writer.hpp"
#include "logical_query_plan/stored_table_node.hpp"
#include "operators/abstract_join_operator.hpp"
#include "operators/get_table.hpp"
#include "operators/table_scan.hpp"
#include "operators/visit_pqp.hpp"
#include "utils/assert.hpp"

namespace opossum {

OperatorFeatureExporter::OperatorFeatureExporter(const std::string& path_to_dir) : _path_to_dir(path_to_dir) {}

void OperatorFeatureExporter::export_to_csv(const std::shared_ptr<const AbstractOperator> op) {
  visit_pqp(op, [&](const auto& node) {
    _export_operator(node);
    return PQPVisitation::VisitInputs;
  });
}

void OperatorFeatureExporter::flush() {
  const auto path = _path_to_dir + "/operators.csv";
  CsvWriter::write(*_output_table, path);
}

void OperatorFeatureExporter::_export_operator(const std::shared_ptr<const AbstractOperator>& op) {
  _current_row = {pmr_string{op->name()},
                  int64_t{0},
                  int64_t{0},
                  int64_t{0},
                  int64_t{0},
                  NULL_VALUE,
                  NULL_VALUE,
                  NULL_VALUE,
                  NULL_VALUE};

  if (op->input_left()) {
    _current_row[1] = static_cast<int64_t>(op->input_left()->performance_data().output_row_count);
  }

  if (op->input_right()) {
    _current_row[2] = static_cast<int64_t>(op->input_right()->performance_data().output_row_count);
  }

  if (op->performance_data().has_output) {
    _current_row[3] = static_cast<int64_t>(op->performance_data().output_row_count);
    _current_row[4] = static_cast<int64_t>(op->performance_data().walltime.count());
  }

  switch (op->type()) {
    case OperatorType::Aggregate:
      _add_aggregate_details(op);
      break;
    case OperatorType::JoinHash:
    case OperatorType::JoinIndex:
    case OperatorType::JoinNestedLoop:
    case OperatorType::JoinSortMerge:
    case OperatorType::JoinVerification:
      _add_join_details(op);
      break;
    default:
      break;
  }

  const auto node = op->lqp_node;
  for (const auto& el : node->node_expressions) {
    visit_expression(el, [&](const auto& expression) {
      if (expression->type == ExpressionType::LQPColumn) {
        const auto column_expression = std::dynamic_pointer_cast<LQPColumnExpression>(expression);

        if (const auto& original_node = column_expression->original_node.lock()) {
          if (original_node->type == LQPNodeType::StoredTable) {
            const auto stored_table_node = std::dynamic_pointer_cast<const StoredTableNode>(original_node);
            const auto table_name = stored_table_node->table_name;
            _current_row[6] = pmr_string{table_name};
            const auto original_column_id = column_expression->original_column_id;
            const auto table = Hyrise::get().storage_manager.get_table(table_name);
            const auto column_name = pmr_string{table->column_names()[original_column_id]};
            _current_row[7] = pmr_string{column_name};

            if (op->type() == OperatorType::TableScan) {
              _add_table_scan_details(op, node, original_node);
            }
          }
        }
      }
      return ExpressionVisitation::VisitArguments;
    });
  }

  _output_table->append(_current_row);
}

void OperatorFeatureExporter::_add_aggregate_details(const std::shared_ptr<const AbstractOperator>& op) {
  DebugAssert(op->type() == OperatorType::Aggregate, "Expected Aggregate");
  _current_row[0] = pmr_string{"Aggregate"};
  _current_row[8] = pmr_string{op->name()};
}

void OperatorFeatureExporter::_add_get_table_details(const std::shared_ptr<const AbstractOperator>& op) {
  DebugAssert(op->type() == OperatorType::GetTable, "Expected GetTable");
  const auto get_table = std::dynamic_pointer_cast<const GetTable>(op);
  _current_row[6] = pmr_string{get_table->table_name()};
}

void OperatorFeatureExporter::_add_join_details(const std::shared_ptr<const AbstractOperator>& op) {
  _current_row[0] = pmr_string{"Join"};
  _current_row[8] = pmr_string{op->name()};
  const auto join = std::dynamic_pointer_cast<const AbstractJoinOperator>(op);
  _current_row[5] = pmr_string{join_mode_to_string.left.at(join->mode())};
}

void OperatorFeatureExporter::_add_table_scan_details(const std::shared_ptr<const AbstractOperator>& op,
                                                      const std::shared_ptr<const AbstractLQPNode>& lqp_node,
                                                      const std::shared_ptr<const AbstractLQPNode>& original_node) {
  DebugAssert(op->type() == OperatorType::TableScan, "Expected TableScan");
  _current_row[5] = original_node == lqp_node->left_input() ? pmr_string{"COLUMN_SCAN"} : pmr_string{"REFERENCE_SCAN"};
  const auto table_scan = std::dynamic_pointer_cast<const TableScan>(op);
  Assert(table_scan->_impl_description != "Unset", "Expected TableScan to be executed.");
  _current_row[8] = pmr_string{table_scan->_impl_description};
}

}  // namespace opossum
