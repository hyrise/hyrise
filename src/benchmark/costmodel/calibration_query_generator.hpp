#pragma once

#include <json.hpp>
#include <string>
#include <vector>

namespace opossum {

    class CalibrationQueryGenerator {

    public:
        static const std::vector<std::string> generate_queries(const nlohmann::json& table_definitions);

    private:
        static const std::string _generate_select_star(const nlohmann::json& table_definition);
        static const std::string _generate_table_scan(const nlohmann::json& table_definition);
        static const std::string _generate_select_columns(const nlohmann::json& column_definitions);

        static const std::vector<std::string> _get_column_names(const nlohmann::json& column_definitions);

        CalibrationQueryGenerator() = default;
    };


}


