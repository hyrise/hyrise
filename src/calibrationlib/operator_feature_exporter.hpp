#pragma once

#include <string>

#include "import_export/csv/csv_writer.hpp"
#include "operators/abstract_operator.hpp"
#include "storage/table.hpp"

namespace opossum {

class OperatorFeatureExporter {
 public:
  explicit OperatorFeatureExporter(const std::string& path_to_dir);

  void export_to_csv(const std::shared_ptr<const AbstractOperator> op);

  void flush();

 protected:
  std::map<OperatorType, std::shared_ptr<Table>> _tables = {
      {OperatorType::TableScan,
       std::make_shared<Table>(OperatorFeatureExporter::_column_definitions.at(OperatorType::TableScan),
                               TableType::Data)}};

  // TODO: make this more generic: every operator should be exported easily
  // Like dumping everything in one table and filter out in Notebook
  static inline std::map<OperatorType, TableColumnDefinitions> _column_definitions = {
      {OperatorType::TableScan, TableColumnDefinitions{{"INPUT_ROWS_LEFT", DataType::Long, true},
                                                       {"OUTPUT_ROWS", DataType::Long, true},
                                                       {"RUNTIME_NS", DataType::Long, true},
                                                       {"SCAN_TYPE", DataType::String, false},
                                                       {"TABLE_NAME", DataType::String, false},
                                                       {"COLUMN_NAME", DataType::String, false},
                                                       {"SCAN_IMPLEMENTATION", DataType::String, false}}}};

 private:
  const std::string& _path_to_dir;

  void _export_typed_operator(const std::shared_ptr<const AbstractOperator>& op,
                              std::unordered_set<std::shared_ptr<const AbstractOperator>>& visited_operators);
  void _export_table_scan(const std::shared_ptr<const AbstractOperator> op);

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
