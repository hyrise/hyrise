#include "cost_model_feature_extractor.hpp"

#include <json.hpp>

namespace opossum {

    const nlohmann::json CostModelFeatureExtractor::extract_constant_hardware_features() {
        nlohmann::json hardware_features{};

        // Hard-coded to MacBook Config
        hardware_features["l1_size_kb"] = 0;
        hardware_features["l1_block_size_kb"] = 0;
        hardware_features["l2_size_kb"] = 256;
        hardware_features["l2_block_size_kb"] = 0;
        hardware_features["l3_size_mb"] = 3;
        hardware_features["l3_block_size_mb"] = 0;

        hardware_features["memory_size_gb"] = 8;
        hardware_features["memory_access_bandwith"] = 0;
        hardware_features["memory_access_latency"] = 0;
        hardware_features["cpu_architecture"] = 0; // Should be ENUM
        hardware_features["num_cpu_cores"] = 2;
        hardware_features["cpu_clock_speed_mhz"] = 2400;
        hardware_features["num_numa_nodes"] = 0;

        return hardware_features;
    }

    const nlohmann::json CostModelFeatureExtractor::extract_runtime_hardware_features() {
        nlohmann::json runtime_features{};

        // Current System Load
        runtime_features["current_memory_consumption_percentage"] = 0;
        runtime_features["running_queries"] = 0;
        runtime_features["remaining_transactions"] = 0;

        return runtime_features;
    }

}  // namespace opossum