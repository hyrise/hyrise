#pragma once

#include <json.hpp>

namespace opossum {

    class CostModelFeatureExtractor {

    public:
        static const nlohmann::json extract_constant_hardware_features();
        static const nlohmann::json extract_runtime_hardware_features();
    };

}  // namespace opossum