#pragma once

#include <json.hpp>

#include <string>

#include "constant_mappings.hpp"
#include "storage/encoding_type.hpp"

namespace opossum {

    // Hard-coded to test server
    struct CalibrationRuntimeHardwareFeatures {
        size_t current_memory_consumption_percentage = 0;
        size_t running_queries = 0;
        size_t remaining_transactions = 0;
    };

    inline void to_json(nlohmann::json& j, const CalibrationRuntimeHardwareFeatures& s) {
        j = nlohmann::json{
                {"current_memory_consumption_percentage", s.current_memory_consumption_percentage},
                {"running_queries", s.running_queries},
                {"remaining_transactions", s.remaining_transactions}
        };
    }

    inline void from_json(const nlohmann::json& j, CalibrationRuntimeHardwareFeatures& s) {
        s.current_memory_consumption_percentage = j.value("current_memory_consumption_percentage", 0);
        s.running_queries = j.value("running_queries", 0);
        s.remaining_transactions = j.value("remaining_transactions", 0);
    }

}  // namespace opossum
