#pragma once

#include <string>

#include <operators/abstract_operator.hpp>
#include "csv_writer.hpp"

namespace opossum {

class OperatorFeatureExporter {
 public:
  explicit OperatorFeatureExporter(const std::string& path_to_dir);

  void export_to_csv(std::shared_ptr<const AbstractOperator> op) const;

 private:
  const std::string& _path_to_dir;

  std::optional<const std::vector<std::string>> _get_header(const OperatorType type) const {
    std::stringstream ss;
    switch (type) {
      case OperatorType::TableScan:
        return std::vector<std::string>({"INPUT_ROWS_LEFT", "OUTPUT_ROWS", "RUNTIME_NS", "SCAN_TYPE", "TABLE_NAME",
                                         "COLUMN_NAME", "SCAN_IMPLEMENTATION"});
      default:
        return {};
    }
  }

  const std::map<OperatorType, std::shared_ptr<CSVWriter>> _csv_writers = [&]() {
    auto csv_writers_per_operator = std::map<OperatorType, std::shared_ptr<CSVWriter>>();

    for (int op_type_id = static_cast<int>(OperatorType::Projection);
         op_type_id != static_cast<int>(OperatorType::Validate); ++op_type_id) {
      const auto op_type = static_cast<OperatorType>(op_type_id);
      const auto headers_for_type = _get_header(op_type);

      if (const auto headers = _get_header(op_type)) {
        std::stringstream path;
        path << _path_to_dir << "/" << _map_operator_type(op_type) << ".csv";
        csv_writers_per_operator.emplace(op_type, std::make_shared<CSVWriter>(CSVWriter(path.str(), *headers)));
      }
    }
    return csv_writers_per_operator;
  }();

  void _export_typed_operator(std::shared_ptr<const AbstractOperator> op) const;
  void _export_table_scan(std::shared_ptr<const AbstractOperator> op) const;

  // TODO(Bouncner): use magic_enum.name when available
  const std::string _map_operator_type(const OperatorType op_type) {
    switch (op_type) {
      case OperatorType::Aggregate:
        return "Aggregate";
      case OperatorType::Alias:
        return "Alias";
      case OperatorType::ChangeMetaTable:
        return "ChangeMetaTable";
      case OperatorType::Delete:
        return "Delete";
      case OperatorType::Difference:
        return "Difference";
      case OperatorType::Export:
        return "Export";
      case OperatorType::GetTable:
        return "GetTable";
      case OperatorType::Import:
        return "Import";
      case OperatorType::IndexScan:
        return "IndexScan";
      case OperatorType::Insert:
        return "Insert";
      case OperatorType::JoinHash:
        return "JoinHash";
      case OperatorType::JoinIndex:
        return "JoinIndex";
      case OperatorType::JoinNestedLoop:
        return "JoinNestedLoop";
      case OperatorType::JoinSortMerge:
        return "JoinSortMerge";
      case OperatorType::JoinVerification:
        return "JoinVerification";
      case OperatorType::Limit:
        return "Limit";
      case OperatorType::Print:
        return "Print";
      case OperatorType::Product:
        return "Product";
      case OperatorType::Projection:
        return "Projection";
      case OperatorType::Sort:
        return "Sort";
      case OperatorType::TableScan:
        return "TableScan";
      case OperatorType::TableWrapper:
        return "TableWrapper";
      case OperatorType::UnionAll:
        return "UnionAll";
      case OperatorType::UnionPositions:
        return "UnionPositions";
      case OperatorType::Update:
        return "Update";
      case OperatorType::Validate:
        return "Validate";
      case OperatorType::CreateTable:
        return "CreateTable";
      case OperatorType::CreatePreparedPlan:
        return "CreatePreparedPlan";
      case OperatorType::CreateView:
        return "CreateView";
      case OperatorType::DropTable:
        return "DropTable";
      case OperatorType::DropView:
        return "DropView";
      case OperatorType::Mock:
        return "Mock";
      default:
        Fail("Requested mapping for unknown OperatorType");
    }
  }
};
}  // namespace opossum
