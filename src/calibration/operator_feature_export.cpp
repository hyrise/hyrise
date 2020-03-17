#include "fstream"

#include <expression/lqp_column_expression.hpp>
#include <expression/expression_utils.hpp>
#include <logical_query_plan/stored_table_node.hpp>
#include <regex>
#include "operator_feature_export.hpp"
#include "constant_mappings.hpp"
#include "storage/table.hpp"
#include "hyrise.hpp"
#include "csv_writer.hpp"

namespace opossum {

    OperatorFeatureExport::OperatorFeatureExport(const std::string& path_to_dir) : _path_to_dir(path_to_dir){
    }

    void OperatorFeatureExport::export_to_csv(std::shared_ptr<const AbstractOperator> op) const {
      if (op != nullptr){
        //Export current operator
        _export_typed_operator(op);

        export_to_csv(op->input_left());
        export_to_csv(op->input_right());

      }
    }

    void OperatorFeatureExport::_export_typed_operator(std::shared_ptr<const AbstractOperator> op) const {
      switch(op->type()){
        case OperatorType::TableScan: _export_table_scan(op); break;
        default: break;
      }
    }

    // TABLE_SCAN
    void OperatorFeatureExport::_export_table_scan(std::shared_ptr<const AbstractOperator> op) const {
      auto csv_writer = _csv_writers.at(op->type());

      const auto node = op->lqp_node;
      // const auto predicate_node = std::dynamic_pointer_cast<const PredicateNode>(node);

      for (const auto& el : node->node_expressions) {

        visit_expression(el, [&](const auto& expression) {
            if (expression->type == ExpressionType::LQPColumn) {

              const auto column_expression = std::dynamic_pointer_cast<LQPColumnExpression>(expression);
              const auto column_reference = column_expression->column_reference;
              const auto original_node = column_reference.original_node();

              if (original_node->type == LQPNodeType::StoredTable) {

                // INPUT_ROWS_LEFT
                if (op->input_left() != nullptr){
                  csv_writer->set_value("INPUT_ROWS_LEFT", op->input_table_left()->row_count());
                } else {
                  csv_writer->set_value("INPUT_ROWS_LEFT", CSVWriter::NA);
                }

                // OUTPUT_ROWS
                if (op->get_output() != nullptr){
                  csv_writer->set_value("OUTPUT_ROWS", op->get_output()->row_count());
                } else {
                  csv_writer->set_value("OUTPUT_ROWS", CSVWriter::NA);
                }

                // RUNTIME_NS
                csv_writer->set_value("RUNTIME_NS", op->performance_data().walltime.count());

                // SCAN_IMPLEMENTATION
                if (original_node == node->left_input()) {
                  csv_writer->set_value("SCAN_TYPE", "COLUMN_SCAN");
                } else {
                  csv_writer->set_value("SCAN_TYPE", "REFERENCE_SCAN");
                }

                // TABLE_NAME
                const auto stored_table_node = std::dynamic_pointer_cast<const StoredTableNode>(original_node);
                const auto& table_name = stored_table_node->table_name;

                csv_writer->set_value("TABLE_NAME", table_name);

                const auto original_column_id = column_reference.original_column_id();

                // COLUMN_NAME
                const auto table = Hyrise::get().storage_manager.get_table(table_name);

                csv_writer->set_value("COLUMN_NAME", table->column_names()[original_column_id]);

                // SCAN_IMPLEMENTATION
                auto description = op->description();
                std::smatch matches;

                std::regex self_regex("Impl: ([A-Z]\\w+)", std::regex_constants::ECMAScript | std::regex_constants::icase);
                if (std::regex_search(description, matches, self_regex)) {
                  csv_writer->set_value("SCAN_IMPLEMENTATION", matches[1]);
                } else {
                  csv_writer->set_value("SCAN_IMPLEMENTATION", CSVWriter::NA);
                }

                csv_writer->write_row();
              }
            }
            return ExpressionVisitation::VisitArguments;
        });
      }
    }
}  // namespace opossum