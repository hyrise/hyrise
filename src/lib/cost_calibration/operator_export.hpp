//
// Created by Lukas Böhme on 03.01.20.
//

#include <operators/abstract_operator.hpp>
#include <string>
#include <operators/table_scan.hpp>

namespace opossum{
class OperatorExport {
 public:
  OperatorExport(const std::string path_to_csv, const std::string delimiter = ",")
  : path_to_csv(path_to_csv), delimiter(delimiter);

  void export_to_csv(const std::shared_ptr<const AbstractOperator> op) const ;

 private:
  const std::string path_to_csv;
  const std::string delimiter;

  //TODO Add constexpr here - constructor of vector supports constexpr in C++20
  std::vector<const std::string> get_header(const std::shared_ptr<const AbstractOperator> op) const;
  std::vector<const std::string> get_header(const std::shared_ptr<const TableScan> op) const;
  void create_file_with_header() const;

  std::shared_ptr<std::vector<const std::string>> get_measurements(const std::shared_ptr<const AbstractOperator> op) const;
  std::shared_ptr<std::vector<const std::string>> get_measurements(const std::shared_ptr<const TableScan> op) const;


};
}