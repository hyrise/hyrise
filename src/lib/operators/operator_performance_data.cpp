#include "operator_performance_data.hpp" // NEEDEDINCLUDE


#include "utils/format_duration.hpp" // NEEDEDINCLUDE

namespace opossum {

std::string OperatorPerformanceData::to_string(DescriptionMode description_mode) const {
  return format_duration(std::chrono::duration_cast<std::chrono::nanoseconds>(walltime));
}

}  // namespace opossum
