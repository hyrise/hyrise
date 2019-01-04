#include "feature/runtime_hardware_features.hpp"

namespace opossum {
namespace cost_model {

const std::map<std::string, AllTypeVariant> RuntimeHardwareFeatures::serialize() const {return {
    {"current_memory_consumption_percentage", static_cast<int32_t>(current_memory_consumption_percentage)},
    {"running_query_count", static_cast<int32_t>(running_query_count)},
    {"remaining_transaction_count", static_cast<int32_t>(remaining_transaction_count)},
}};

}  // namespace cost_model
}  // namespace opossum