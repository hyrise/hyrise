#include "operator_performance_data.hpp"

#include <ostream>

namespace hyrise {

std::ostream& operator<<(std::ostream& stream, const AbstractOperatorPerformanceData& performance_data) {
  performance_data.output_to_stream(stream, DescriptionMode::SingleLine);
  return stream;
}

}  // namespace hyrise
