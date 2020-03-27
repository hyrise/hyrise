#pragma once

#include <string>

#include <operators/abstract_operator.hpp>
#include "constant_mappings.hpp"
#include "csv_writer.hpp"

namespace opossum {
class OperatorFeatureExport {
 public:
  explicit OperatorFeatureExport(const std::string& path_to_dir);

  void export_to_csv(std::shared_ptr<const AbstractOperator> op) const;

 private:
  const std::string& _path_to_dir;

  [[nodiscard]] std::optional<const std::vector<std::string>> _get_header(const OperatorType type) const {
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

    // TODO(Martin): How to iterate over enums in a safe way?
    for (int op_type_id = static_cast<int>(OperatorType::Projection);
         op_type_id != static_cast<int>(OperatorType::Validate); ++op_type_id) {
      const auto op_type = static_cast<OperatorType>(op_type_id);
      const auto headers_for_type = _get_header(op_type);

      if (const auto headers = _get_header(op_type)) {
        std::stringstream path;
        path << _path_to_dir << "/" << op_type << ".csv";
        csv_writers_per_operator.emplace(op_type, std::make_shared<CSVWriter>(CSVWriter(path.str(), *headers)));
      }
    }
    return csv_writers_per_operator;
  }();

  void _export_typed_operator(std::shared_ptr<const AbstractOperator> op) const;
  void _export_table_scan(std::shared_ptr<const AbstractOperator> op) const;
};
}  // namespace opossum
