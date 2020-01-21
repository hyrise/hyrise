//
// Created by Lukas Böhme on 03.01.20.
//
#include "fstream"

#include <expression/lqp_column_expression.hpp>
#include <expression/expression_utils.hpp>
#include <logical_query_plan/stored_table_node.hpp>
#include "measurement_export.hpp"
#include "constant_mappings.hpp"
#include "storage/table.hpp"
#include "hyrise.hpp"

namespace opossum {

    MeasurementExport::MeasurementExport(const std::string& path_to_dir) : _path_to_dir(path_to_dir){
      _create_file_table_scan();
    }

    void MeasurementExport::export_to_csv(std::shared_ptr<const AbstractOperator> op) const {
      if (op != nullptr){
        //Export current operator
        _export_typed_operator(op);

        export_to_csv(op->input_left());
        export_to_csv(op->input_right());

      }
    }

    void MeasurementExport::_export_typed_operator(std::shared_ptr<const AbstractOperator> op) const {
      switch(op->type()){
        case OperatorType::Aggregate:           break;
        case OperatorType::Alias:               break;
        case OperatorType::Delete:              break;
        case OperatorType::Difference:          break;
        case OperatorType::ExportBinary:        break;
        case OperatorType::ExportCsv:           break;
        case OperatorType::GetTable:            break;
        case OperatorType::ImportBinary:        break;
        case OperatorType::ImportCsv:           break;
        case OperatorType::IndexScan:           break;
        case OperatorType::Insert:              break;
        case OperatorType::JoinHash:            break;
        case OperatorType::JoinIndex:           break;
        case OperatorType::JoinNestedLoop:      break;
        case OperatorType::JoinSortMerge:       break;
        case OperatorType::JoinVerification:    break;
        case OperatorType::Limit:               break;
        case OperatorType::Print:               break;
        case OperatorType::Product:             break;
        case OperatorType::Projection:          break;
        case OperatorType::Sort:                break;
        case OperatorType::TableScan:           _export_table_scan(op); break;
        case OperatorType::TableWrapper:        break;
        case OperatorType::UnionAll:            break;
        case OperatorType::UnionPositions:      break;
        case OperatorType::Update:              break;
        case OperatorType::Validate:            break;
        case OperatorType::CreateTable:         break;
        case OperatorType::CreatePreparedPlan:  break;
        case OperatorType::CreateView:          break;
        case OperatorType::DropTable:           break;
        case OperatorType::DropView:            break;
        case OperatorType::Mock:                break;
      }
    }

    //TODO Construct path before starting export-> save it in map
    std::string MeasurementExport::_path_by_type(OperatorType operator_type) const {
      std::stringstream path;
      path << _path_to_dir << "/" << operator_type << ".csv";
      return path.str();
    }

    void MeasurementExport::_append_to_file_by_operator_type(std::string line, OperatorType operator_type) const {
      std::ofstream outfile;
      //TODO Get path from map
      outfile.open(_path_by_type(operator_type), std::ofstream::out | std::ofstream::app);
      outfile << line;
      outfile.close();
    }

    // TABLE_SCAN
    void MeasurementExport::_export_table_scan(std::shared_ptr<const AbstractOperator> op) const {
      std::stringstream ss;

      const auto node = op->lqp_node;
      // const auto predicate_node = std::dynamic_pointer_cast<const PredicateNode>(node);

      for (const auto& el : node->node_expressions) {

        visit_expression(el, [&](const auto& expression) {
            if (expression->type == ExpressionType::LQPColumn) {

              // INPUT_ROWS_LEFT TODO this does not extract anything
              ss << (op->input_left() != nullptr ? std::to_string(op->input_table_left()->row_count()) : "null") << _delimiter;

              // OUTPUT_ROWS
              ss << (op->get_output() != nullptr ? std::to_string(op->get_output()->row_count()) : "null") << _delimiter;

              // RUNTIME_NS
              ss << std::to_string(op->performance_data().walltime.count()) << _delimiter;

              const auto column_expression = std::dynamic_pointer_cast<LQPColumnExpression>(expression);
              const auto column_reference = column_expression->column_reference;
              const auto original_node = column_reference.original_node();

              if (original_node->type == LQPNodeType::StoredTable) {

                // SCAN_TYPE
                if (original_node == node->left_input()) {
                  ss << "COLUMN_SCAN";
                } else {
                  ss << "REFERENCE_SCAN";
                }
                ss << _delimiter;

                // TABLE_NAME
                const auto stored_table_node = std::dynamic_pointer_cast<const StoredTableNode>(original_node);
                const auto& table_name = stored_table_node->table_name;
                ss << table_name << _delimiter;
                const auto original_column_id = column_reference.original_column_id();

                // COLUMN_NAME
                const auto sm_table = Hyrise::get().storage_manager.get_table(table_name);
                ss << sm_table->column_names()[original_column_id] << "\n";
              }
            }
            return ExpressionVisitation::VisitArguments;
        });
      }
      _append_to_file_by_operator_type(ss.str(), op->type());
    }

    void MeasurementExport::_create_file_table_scan() const{
      std::stringstream ss;

      ss << "INPUT_ROWS_LEFT" << _delimiter;
      ss << "OUTPUT_ROWS"     << _delimiter;
      ss << "RUNTIME_NS"      << _delimiter;
      ss << "SCAN_TYPE"       << _delimiter;
      ss << "TABLE_NAME"      << _delimiter;
      ss << "COLUMN_NAME"     << "\n";

      _append_to_file_by_operator_type(ss.str(), OperatorType::TableScan);
    }
}  // namespace opossum