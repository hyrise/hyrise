#include "operator_performance_data.hpp"

#include <string>

#include "utils/format_duration.hpp"

namespace opossum {

void OperatorPerformanceData::output_to_stream(std::ostream& stream, DescriptionMode description_mode) const {
  stream << format_duration(std::chrono::duration_cast<std::chrono::nanoseconds>(walltime));
}

}  // namespace opossum
