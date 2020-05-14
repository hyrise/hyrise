#include "operator_feature_exporter.hpp"
#include <boost/algorithm/string.hpp>
#include <expression/expression_utils.hpp>
#include <expression/lqp_column_expression.hpp>
#include <logical_query_plan/stored_table_node.hpp>
#include <utils/assert.hpp>
#include "csv_writer.hpp"
#include "hyrise.hpp"
#include "storage/table.hpp"

namespace opossum {

OperatorFeatureExporter::OperatorFeatureExporter(const std::string& path_to_dir) : _path_to_dir(path_to_dir) {}

void OperatorFeatureExporter::export_to_csv(const std::shared_ptr<const AbstractOperator> op) {
  std::unordered_set<std::shared_ptr<const AbstractOperator>> visited_operators;
  _export_typed_operator(op, visited_operators);
}

void OperatorFeatureExporter::flush() {
  for (auto& [op_type, table] : _tables) {
    std::stringstream path;
    path << _path_to_dir << "/" << _map_operator_type(op_type) << "_new.csv";
    CsvWriter::write(*table, path.str());
    _tables[op_type] = std::make_shared<Table>(_column_definitions.at(op_type), TableType::Data);
  }
}

void OperatorFeatureExporter::_export_typed_operator(
    const std::shared_ptr<const AbstractOperator>& op,
    std::unordered_set<std::shared_ptr<const AbstractOperator>>& visited_operators) {
  switch (op->type()) {
    case OperatorType::TableScan:
      _export_table_scan(op);
      break;
    default:
      break;
  }

  visited_operators.insert(op);

  auto left_input = op->input_left();
  if (left_input && !visited_operators.contains(left_input)) {
    _export_typed_operator(left_input, visited_operators);
  }

  auto right_input = op->input_right();
  if (right_input && !visited_operators.contains(right_input)) {
    _export_typed_operator(right_input, visited_operators);
  }
}

// Export features of a table scan operator
void OperatorFeatureExporter::_export_table_scan(const std::shared_ptr<const AbstractOperator> op) {
  DebugAssert(op->type() == OperatorType::TableScan, "Expected operator of type: TableScan but got another one");

  auto output_table = _tables.at(op->type());

  const auto node = op->lqp_node;

  for (const auto& el : node->node_expressions) {
    visit_expression(el, [&](const auto& expression) {
      if (expression->type == ExpressionType::LQPColumn) {
        const auto column_expression = std::dynamic_pointer_cast<LQPColumnExpression>(expression);

        if (const auto& original_node = column_expression->original_node.lock()) {
          if (original_node->type == LQPNodeType::StoredTable) {
            const auto input_rows =
                op->input_left()
                    ? AllTypeVariant{static_cast<int64_t>(op->input_left()->performance_data().output_row_count)}
                    : NULL_VALUE;
            AllTypeVariant output_rows = NULL_VALUE;
            AllTypeVariant runtime_ms = NULL_VALUE;

            if (op->performance_data().has_output) {
              output_rows = AllTypeVariant{static_cast<int64_t>(op->performance_data().output_row_count)};
              runtime_ms = AllTypeVariant{static_cast<int64_t>(op->performance_data().walltime.count())};
            }

            const auto scan_type = original_node == node->left_input() ? AllTypeVariant{pmr_string{"COLUMN_SCAN"}}
                                                                       : AllTypeVariant{pmr_string{"REFERENCE_SCAN"}};

            const auto stored_table_node = std::dynamic_pointer_cast<const StoredTableNode>(original_node);
            const auto table_name = stored_table_node->table_name;

            const auto original_column_id = column_expression->original_column_id;

            const auto table = Hyrise::get().storage_manager.get_table(table_name);
            const auto column_name = pmr_string{table->column_names()[original_column_id]};

            // Example Description: TableScan Impl: ColumnVsValue c_acctbal > SUBQUERY (PQP, 0x179d55098)
            // We need the scan implementation of the string above (here: ColumnVsValue)
            const auto description = op->description();

            // Split the description by " " and extract the word with index 2
            std::vector<std::string> description_values;
            boost::split(description_values, description, boost::is_any_of(" "));

            Assert(description_values[1] == "Impl:", "Did not find implementation type in string.");
            const auto implementation = pmr_string{description_values[2]};

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
