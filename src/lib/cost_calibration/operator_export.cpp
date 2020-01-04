//
// Created by Lukas Böhme on 03.01.20.
//

#include <fstream>
#include "operator_export.hpp"

namespace opossum {

    OperatorExport::OperatorExport(const std::string path_to_csv, std::string delimiter) :
    path_to_csv(path_to_csv), delimiter(delimiter){
      //TODO Create file and add header if not exists.
      //TODO If file exists check if header is correct.
    }

    void OperatorExport::export_to_csv(const std::shared_ptr<const AbstractOperator> op) {
      //Open stream to related file
      std::ofstream output_performance_data_file;
      output_performance_data_file.open (path_to_csv);

      std::shared_ptr<std::vector<const std::string>> measurements = get_measurements(op);

      //Add all
      int columns_with_delimiter= int(measurements->size() - 1);  // Is this correct?
      for (int i = 0; i < columns_with_delimiter; ++i){
        output_performance_data_file << measurements->operator[](i) << delimiter;
      }

      //Add last column without delimiter
      output_performance_data_file << measurements->operator[](columns_with_delimiter + 1);
      output_performance_data_file.close();
    }

    //TODO this is ugly code for an easy problem -> constexpr and
    // / or outsource const expressions because we dont want those in all export operators
    std::vector<const std::string>
    OperatorExport::get_header(const std::shared_ptr<const AbstractOperator> op) const {
      std::vector<const std::string> header {"INSERT", "HEADER"};
      return header;
    }

    std::vector<const std::string>
    OperatorExport::get_header(const std::shared_ptr<const TableScan> op) const {
      std::vector<const std::string> header {"INSERT", "HEADER"};
      return header;
    }

    void OperatorExport::create_file_with_header() const {

    }

    std::shared_ptr<std::vector<const std::string>> OperatorExport::get_measurements(const std::shared_ptr<const AbstractOperator> op) {
      return std::shared_ptr<std::vector<const std::string>>();
    }

    std::shared_ptr<std::vector<const std::string>> OperatorExport::get_measurements(const std::shared_ptr<const TableScan> op) {
      return std::shared_ptr<std::vector<const std::string>>();
    }
}  // namespace opossum