#include "calibration_query_generator.hpp"

#include <experimental/iterator>

namespace opossum {

    const std::string CalibrationQueryGenerator::generate_select_star(const nlohmann::json& table_definition) {
        std::vector<std::string> column_names;

        auto o = table_definition["columns"];
        for (auto it = o.begin(); it != o.end(); ++it) { column_names.push_back(it.key()); }

        std::ostringstream stringstream;
        stringstream << "SELECT ";

        std::copy(std::begin(column_names),
                  std::end(column_names),
                  std::experimental::make_ostream_joiner(stringstream, ", "));
        stringstream << " FROM " << table_definition["table_name"].get<std::string>() << ";";
        return stringstream.str();
    }

    const std::string CalibrationQueryGenerator::generate_table_scan(const nlohmann::json& table_definition) {
      return "";
    }
}