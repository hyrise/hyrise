#include "calibration_table_specification.hpp"

#include "calibration_column_specification.hpp"

namespace opossum {

//    CalibrationTableSpecification::CalibrationTableSpecification(
//            const std::string table_path,
//            const std::string table_name,
//            const int table_size,
//            const std::map<std::string, CalibrationColumnSpecification> columns): table_path(table_path), table_name(table_name), table_size(table_size), columns(columns) {}

//    CalibrationTableSpecification CalibrationTableSpecification::parse_json_configuration(const nlohmann::json& configuration) {
//      const auto table_path = configuration["table_path"];
//      const auto table_name = configuration["table_name"];
//      const auto table_size = configuration["table_size"];
//
//      const auto json_columns = configuration["columns"];
//      std::map<std::string, CalibrationColumnSpecification> parsed_columns;
//      for (auto it = json_columns.begin(); it != json_columns.end(); ++it) {
//        auto column_name = it.key();
//        auto column = CalibrationColumnSpecification::parse_json_configuration(it.value());
//        parsed_columns.insert(std::pair<std::string, CalibrationColumnSpecification>(column_name, column));
//      }
//
//      return CalibrationTableSpecification(table_path, table_name, table_size, parsed_columns);
//    }

}  // namespace opossum