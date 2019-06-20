#include "constant_hardware_features.hpp"

namespace opossum {
namespace cost_model {

const std::map<std::string, AllTypeVariant> ConstantHardwareFeatures::serialize() const {
  return {
      {"l1_size_kb", static_cast<int32_t>(l1_size_kb)},
      {"l1_block_size_kb", static_cast<int32_t>(l1_block_size_kb)},
      {"l2_size_kb", static_cast<int32_t>(l2_size_kb)},
      {"l2_block_size_kb", static_cast<int32_t>(l2_block_size_kb)},
      {"l3_size_kb", static_cast<int32_t>(l3_size_kb)},
      {"l3_block_size_kb", static_cast<int32_t>(l3_block_size_kb)},
      {"memory_size_kb", static_cast<int32_t>(memory_size_kb)},
      {"memory_access_bandwidth", static_cast<int32_t>(memory_access_bandwidth)},
      {"memory_access_latency", static_cast<int32_t>(memory_access_latency)},
      {"cpu_core_count", static_cast<int32_t>(cpu_core_count)},
      {"cpu_clock_speed_mhz", static_cast<int32_t>(cpu_clock_speed_mhz)},
      {"numa_node_count", static_cast<int32_t>(numa_node_count)},
      {"cpu_architecture", static_cast<int32_t>(cpu_architecture)},
  };
}

const std::unordered_map<std::string, float> ConstantHardwareFeatures::to_cost_model_features() const {
  return {
      {"l1_size_kb", static_cast<float>(l1_size_kb)},
      {"l1_block_size_kb", static_cast<float>(l1_block_size_kb)},
      {"l2_size_kb", static_cast<float>(l2_size_kb)},
      {"l2_block_size_kb", static_cast<float>(l2_block_size_kb)},
      {"l3_size_kb", static_cast<float>(l3_size_kb)},
      {"l3_block_size_kb", static_cast<float>(l3_block_size_kb)},
      {"memory_size_kb", static_cast<float>(memory_size_kb)},
      {"memory_access_bandwidth", static_cast<float>(memory_access_bandwidth)},
      {"memory_access_latency", static_cast<float>(memory_access_latency)},
      {"cpu_core_count", static_cast<float>(cpu_core_count)},
      {"cpu_clock_speed_mhz", static_cast<float>(cpu_clock_speed_mhz)},
      {"numa_node_count", static_cast<float>(numa_node_count)},
      {"cpu_architecture", static_cast<float>(cpu_architecture)},
  };
}

}  // namespace cost_model
}  // namespace opossum