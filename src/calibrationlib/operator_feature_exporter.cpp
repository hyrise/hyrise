#include "operator_feature_exporter.hpp"

#include <boost/algorithm/string.hpp>

#include "expression/expression_utils.hpp"
#include "expression/lqp_column_expression.hpp"
#include "hyrise.hpp"
#include "logical_query_plan/stored_table_node.hpp"
#include "operators/table_scan.hpp"
#include "operators/visit_pqp.hpp"
#include "storage/table.hpp"
#include "utils/assert.hpp"

namespace opossum {

OperatorFeatureExporter::OperatorFeatureExporter(const std::string& path_to_dir) : _path_to_dir(path_to_dir) {}

void OperatorFeatureExporter::export_to_csv(const std::shared_ptr<const AbstractOperator> op) {
  visit_pqp(op, [&](const auto& node){
    _export_typed_operator(node);
    return PQPVisitation::VisitInputs;
  });
}

void OperatorFeatureExporter::flush() {
  for (auto& [op_type, table] : _tables) {
    std::stringstream path;
    path << _path_to_dir << "/" << _map_operator_type(op_type) << ".csv";
    CsvWriter::write(*table, path.str());
  }
}

void OperatorFeatureExporter::_export_typed_operator(const std::shared_ptr<const AbstractOperator>& op) {
  switch (op->type()) {
    case OperatorType::TableScan:
      _export_table_scan(op);
      break;
    default:
      break;
  }
}

// Export features of a table scan operator
void OperatorFeatureExporter::_export_table_scan(const std::shared_ptr<const AbstractOperator>& op) {
  DebugAssert(op->type() == OperatorType::TableScan, "Expected operator of type: TableScan but got another one");
  auto output_table = _tables.at(op->type());
  const auto node = op->lqp_node;

  for (const auto& el : node->node_expressions) {
    visit_expression(el, [&](const auto& expression) {
      if (expression->type == ExpressionType::LQPColumn) {
        const auto column_expression = std::dynamic_pointer_cast<LQPColumnExpression>(expression);

        if (const auto& original_node = column_expression->original_node.lock()) {
          if (original_node->type == LQPNodeType::StoredTable) {
            const AllTypeVariant input_rows =
                op->input_left() ? static_cast<int64_t>(op->input_left()->performance_data().output_row_count)
                                 : NULL_VALUE;
            AllTypeVariant output_rows = NULL_VALUE;
            AllTypeVariant runtime_ms = NULL_VALUE;

            if (op->performance_data().has_output) {
              output_rows = static_cast<int64_t>(op->performance_data().output_row_count);
              runtime_ms = static_cast<int64_t>(op->performance_data().walltime.count());
            }

            const auto scan_type =
                original_node == node->left_input() ? pmr_string{"COLUMN_SCAN"} : pmr_string{"REFERENCE_SCAN"};

            const auto stored_table_node = std::dynamic_pointer_cast<const StoredTableNode>(original_node);
            const auto table_name = stored_table_node->table_name;
            const auto original_column_id = column_expression->original_column_id;
            const auto table = Hyrise::get().storage_manager.get_table(table_name);
            const auto column_name = pmr_string{table->column_names()[original_column_id]};

            const auto table_scan = std::dynamic_pointer_cast<const TableScan>(op);
            Assert(table_scan->_impl_description != "Unset", "Expected TableScan to be executed.");
            const auto implementation = pmr_string{table_scan->_impl_description};

            output_table->append(
                {input_rows, output_rows, runtime_ms, scan_type, pmr_string{table_name}, column_name, implementation});
          }
        }
      }
      return ExpressionVisitation::VisitArguments;
    });
  }
}
}  // namespace opossum
