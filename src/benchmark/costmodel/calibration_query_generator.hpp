#pragma once

#include <json.hpp>
#include <string>
#include <vector>

namespace opossum {

    class CalibrationQueryGenerator {

    public:
        static const std::string generate_select_star(const nlohmann::json& table_definition);
        static const std::string generate_table_scan(const nlohmann::json& table_definition);

    private:
        CalibrationQueryGenerator() {}
    };


}


