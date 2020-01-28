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
      for ( int type_int = int(OperatorType::Aggregate); type_int !=  int(OperatorType::Mock); ++type_int)
      {
        auto type = static_cast<OperatorType>(type_int);
        _append_to_file(_get_header(type), type);
      }
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
        case OperatorType::Aggregate:           _export_generic(op);    break;
        case OperatorType::Alias:               _export_generic(op);    break;
        case OperatorType::Delete:              _export_generic(op);    break;
        case OperatorType::Difference:          _export_generic(op);    break;
        case OperatorType::ExportBinary:        _export_generic(op);    break;
        case OperatorType::ExportCsv:           _export_generic(op);    break;
        case OperatorType::GetTable:            _export_generic(op);    break;
        case OperatorType::ImportBinary:        _export_generic(op);    break;
        case OperatorType::ImportCsv:           _export_generic(op);    break;
        case OperatorType::IndexScan:           _export_generic(op);    break;
        case OperatorType::Insert:              _export_generic(op);    break;
        case OperatorType::JoinHash:            _export_generic(op);    break;
        case OperatorType::JoinIndex:           _export_generic(op);    break;
        case OperatorType::JoinNestedLoop:      _export_generic(op);    break;
        case OperatorType::JoinSortMerge:       _export_generic(op);    break;
        case OperatorType::JoinVerification:    _export_generic(op);    break;
        case OperatorType::Limit:               _export_generic(op);    break;
        case OperatorType::Print:               _export_generic(op);    break;
        case OperatorType::Product:             _export_generic(op);    break;
        case OperatorType::Projection:          _export_generic(op);    break;
        case OperatorType::Sort:                _export_generic(op);    break;
        case OperatorType::TableScan:           _export_table_scan(op); break;
        case OperatorType::TableWrapper:        _export_generic(op);    break;
        case OperatorType::UnionAll:            _export_generic(op);    break;
        case OperatorType::UnionPositions:      _export_generic(op);    break;
        case OperatorType::Update:              _export_generic(op);    break;
        case OperatorType::Validate:            _export_generic(op);    break;
        case OperatorType::CreateTable:         _export_generic(op);    break;
        case OperatorType::CreatePreparedPlan:  _export_generic(op);    break;
        case OperatorType::CreateView:          _export_generic(op);    break;
        case OperatorType::DropTable:           _export_generic(op);    break;
        case OperatorType::DropView:            _export_generic(op);    break;
        case OperatorType::Mock:                _export_generic(op);    break;
      }
    }

    //TODO Construct path before starting export-> save it in map
    std::string MeasurementExport::_path_by_type(OperatorType operator_type) const {
      std::stringstream path;
      path << _path_to_dir << "/" << operator_type << ".csv";
      return path.str();
    }

    void MeasurementExport::_append_to_file(std::string line, OperatorType operator_type) const {
      std::ofstream outfile;
      //TODO Get path from map
      outfile.open(_path_by_type(operator_type), std::ofstream::out | std::ofstream::app);
      outfile << line;
      outfile.close();
    }

    void MeasurementExport::_export_generic(std::shared_ptr<const AbstractOperator> op) const {
      std::stringstream ss;
      // INPUT_ROWS_LEFT TODO this does not extract anything
      ss << (op->input_left() != nullptr ? std::to_string(op->input_table_left()->row_count()) : "null") << _separator;

      // INPUT_ROWS_LEFT TODO this does not extract anything
      ss << (op->input_right() != nullptr ? std::to_string(op->input_table_right()->row_count()) : "null") << _separator;

      // OUTPUT_ROWS
      ss << (op->get_output() != nullptr ? std::to_string(op->get_output()->row_count()) : "null") << _separator;

      // RUNTIME_NS
      ss << std::to_string(op->performance_data().walltime.count()) << "\n";

      _append_to_file(ss.str(), op->type());
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
              ss << (op->input_left() != nullptr ? std::to_string(op->input_table_left()->row_count()) : "null") << _separator;

              // OUTPUT_ROWS
              ss << (op->get_output() != nullptr ? std::to_string(op->get_output()->row_count()) : "null") << _separator;

              // RUNTIME_NS
              ss << std::to_string(op->performance_data().walltime.count()) << _separator;

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
                ss << _separator;

                // TABLE_NAME
                const auto stored_table_node = std::dynamic_pointer_cast<const StoredTableNode>(original_node);
                const auto& table_name = stored_table_node->table_name;
                ss << table_name << _separator;
                const auto original_column_id = column_reference.original_column_id();

                // COLUMN_NAME
                const auto sm_table = Hyrise::get().storage_manager.get_table(table_name);
                ss << sm_table->column_names()[original_column_id] << "\n";
              }
            }
            return ExpressionVisitation::VisitArguments;
        });
      }
      _append_to_file(ss.str(), op->type());
    }

    std::string MeasurementExport::_get_header(const OperatorType type) const {
      std::stringstream ss;
      switch(type){
        case OperatorType::Aggregate:           return _generic_header();
        case OperatorType::Alias:               return _generic_header();
        case OperatorType::Delete:              return _generic_header();
        case OperatorType::Difference:          return _generic_header();
        case OperatorType::ExportBinary:        return _generic_header();
        case OperatorType::ExportCsv:           return _generic_header();
        case OperatorType::GetTable:            return _generic_header();
        case OperatorType::ImportBinary:        return _generic_header();
        case OperatorType::ImportCsv:           return _generic_header();
        case OperatorType::IndexScan:           return _generic_header();
        case OperatorType::Insert:              return _generic_header();
        case OperatorType::JoinHash:            return _generic_header();
        case OperatorType::JoinIndex:           return _generic_header();
        case OperatorType::JoinNestedLoop:      return _generic_header();
        case OperatorType::JoinSortMerge:       return _generic_header();
        case OperatorType::JoinVerification:    return _generic_header();
        case OperatorType::Limit:               return _generic_header();
        case OperatorType::Print:               return _generic_header();
        case OperatorType::Product:             return _generic_header();
        case OperatorType::Projection:          return _generic_header();
        case OperatorType::Sort:                return _generic_header();
        case OperatorType::TableScan:
          ss << "INPUT_ROWS_LEFT" << _separator;
          ss << "OUTPUT_ROWS"     << _separator;
          ss << "RUNTIME_NS"      << _separator;
          ss << "SCAN_TYPE"       << _separator;
          ss << "TABLE_NAME"      << _separator;
          ss << "COLUMN_NAME"     << "\n";
          break;
        case OperatorType::TableWrapper:        return _generic_header();
        case OperatorType::UnionAll:            return _generic_header();
        case OperatorType::UnionPositions:      return _generic_header();
        case OperatorType::Update:              return _generic_header();
        case OperatorType::Validate:            return _generic_header();
        case OperatorType::CreateTable:         return _generic_header();
        case OperatorType::CreatePreparedPlan:  return _generic_header();
        case OperatorType::CreateView:          return _generic_header();
        case OperatorType::DropTable:           return _generic_header();
        case OperatorType::DropView:            return _generic_header();
        case OperatorType::Mock:                return _generic_header();
      }

      return ss.str();
    }

    std::string MeasurementExport::_generic_header() const {
      std::stringstream ss;

      ss << "INPUT_ROWS_LEFT"   << _separator;
      ss << "INPUT_ROWS_RIGHT"  << _separator;
      ss << "OUTPUT_ROWS"       << _separator;
      ss << "RUNTIME_NS"        << "\n";

      return ss.str();
    }
}  // namespace opossum