//
// Created by Lukas Böhme on 03.01.20.
//

#include "measurement_export.hpp"

namespace opossum {

    void MeasurementSave::export_to_csv(std::shared_ptr<const AbstractOperator> op) const {
      if (op != nullptr){
        export_specific_operator(op);

        while (op->input_left() != nullptr && op->input_right() != nullptr){
          export_to_csv(op->input_left());
          export_to_csv(op->input_right());
        }
      }
    }

    void MeasurementSave::export_specific_operator(std::shared_ptr<const AbstractOperator> op) const {
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
        case OperatorType::TableScan:
          break;
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
}  // namespace opossum