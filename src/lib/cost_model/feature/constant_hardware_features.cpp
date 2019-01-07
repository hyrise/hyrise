#include "constant_hardware_features.hpp"

namespace opossum {
namespace cost_model {

const std::map<std::string, AllTypeVariant> ConstantHardwareFeatures::serialize() const {return {
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
};};

}  // namespace cost_model
}  // namespace opossum