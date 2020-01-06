//
// Created by Lukas Böhme on 03.01.20.
//

#include <cost_calibration/operator_measurements/abstract_operator_measurements.hpp>
#include "measurement_export.hpp"
#include "constant_mappings.hpp"

namespace opossum {

    void MeasurementExport::export_to_csv(std::shared_ptr<const AbstractOperator> op) const {
      if (op != nullptr){
        //Export current operator
        export_typed_operator(op);

        // Recursion on left & right operator
        while (op->input_left() != nullptr && op->input_right() != nullptr){
          export_to_csv(op->input_left());
          export_to_csv(op->input_right());
        }
      }
    }

    void MeasurementExport::export_typed_operator(std::shared_ptr<const AbstractOperator> op) const {
      switch(op->type()){
        case OperatorType::Aggregate:
          break;
        case OperatorType::Alias:
          break;
        case OperatorType::Delete:
          break;
        case OperatorType::Difference:
          break;
        case OperatorType::ExportBinary:
          break;
        case OperatorType::ExportCsv:
          break;
        case OperatorType::GetTable:
          break;
        case OperatorType::ImportBinary:
          break;
        case OperatorType::ImportCsv:
          break;
        case OperatorType::IndexScan:
          break;
        case OperatorType::Insert:
          break;
        case OperatorType::JoinHash:
          break;
        case OperatorType::JoinIndex:
          break;
        case OperatorType::JoinNestedLoop:
          break;
        case OperatorType::JoinSortMerge:
          break;
        case OperatorType::JoinVerification:
          break;
        case OperatorType::Limit:
          break;
        case OperatorType::Print:
          break;
        case OperatorType::Product:
          break;
        case OperatorType::Projection:
          break;
        case OperatorType::Sort:
          break;
        case OperatorType::TableScan: {
          auto op_measurements = AbstractOperatorMeasurements(op); //TODO Change to TableScanOperatorMeasurements as soon as we get the info from martin
          op_measurements.export_to_csv(path_to_dir + "/table_scan.csv");
          break;
        }
        case OperatorType::TableWrapper:
          break;
        case OperatorType::UnionAll:
          break;
        case OperatorType::UnionPositions:
          break;
        case OperatorType::Update:
          break;
        case OperatorType::Validate:
          break;
        case OperatorType::CreateTable:
          break;
        case OperatorType::CreatePreparedPlan:
          break;
        case OperatorType::CreateView:
          break;
        case OperatorType::DropTable:
          break;
        case OperatorType::DropView:
          break;
        case OperatorType::Mock:
          break;
      }
    }

    MeasurementExport::MeasurementExport(std::string path_to_dir) : path_to_dir(path_to_dir){

    }

    //TODO Construct path before starting export-> save it in map
    std::string MeasurementExport::get_path_by_type(OperatorType operator_type) {
      std::stringstream path;
      path << path_to_dir << operator_type << ".csv";
      return path.str();
    }
}  // namespace opossum