#include "operator_performance_data.hpp"

#include <string>

#include "utils/format_duration.hpp"

namespace opossum {

std::ostream& operator<<(std::ostream& stream, const AbstractOperatorPerformanceData& performance_data) {
  performance_data.output_to_stream(stream, DescriptionMode::SingleLine);
  return stream;
}

}  // namespace opossum
