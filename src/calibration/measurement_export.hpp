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
  const std::string _separator = ",";

  void _export_typed_operator(std::shared_ptr<const AbstractOperator> op) const;

  [[nodiscard]] std::string _path_by_type(OperatorType operator_type) const ;

  void _export_generic(std::shared_ptr<const AbstractOperator> op) const;

  void _export_table_scan(std::shared_ptr<const AbstractOperator> op) const;

  void _append_to_file(std::string line, OperatorType operator_type) const;

  std::string _get_header(const OperatorType type) const;

  std::string _generic_header() const;
};
}  // namespace opossum
