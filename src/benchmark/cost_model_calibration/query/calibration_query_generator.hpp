#pragma once

#include <json.hpp>
#include <string>
#include <vector>

#include "../configuration/calibration_column_specification.hpp"
#include "../configuration/calibration_table_specification.hpp"

namespace opossum {

    class CalibrationQueryGenerator {

    public:
        static const std::vector<std::string> generate_queries(const std::vector<CalibrationTableSpecification>& table_definitions);

    private:
        static const std::string _generate_select_star(const CalibrationTableSpecification& table_definition);
        static const std::string _generate_table_scan(const CalibrationTableSpecification& table_definition);
        static const std::string _generate_select_columns(const std::map<std::string, CalibrationColumnSpecification>& column_definitions);

        static const std::vector<std::string> _get_column_names(const std::map<std::string, CalibrationColumnSpecification>& column_definitions);
        static const std::string _generate_table_scan_predicate(const CalibrationColumnSpecification& column_definition);

        CalibrationQueryGenerator() = default;
    };


}


