#pragma once

#include <operators/abstract_operator.hpp>
#include <string>

namespace opossum {
class MeasurementExport {
 public:
  explicit MeasurementExport(const std::string& path_to_dir);

  void export_to_csv(std::shared_ptr<const AbstractOperator> op) const;

 private:
  const std::string& _path_to_dir;
  //TODO use parserconfig
  const std::string _delimiter = ",";

  void _export_typed_operator(std::shared_ptr<const AbstractOperator> op) const;

  [[nodiscard]] std::string _path_by_type(OperatorType operator_type) const ;

  void _export_table_scan(std::shared_ptr<const AbstractOperator> op) const;

  void _append_to_file_by_operator_type(std::string line, OperatorType operator_type) const;

  void _create_file_table_scan() const;
};
}  // namespace opossum
