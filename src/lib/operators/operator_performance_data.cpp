#include "operator_performance_data.hpp"

#include <string>

#include "utils/format_duration.hpp"

namespace opossum {

void OperatorPerformanceData::output_to_stream(std::ostream& stream, DescriptionMode description_mode) const {
  stream << format_duration(std::chrono::duration_cast<std::chrono::nanoseconds>(walltime));
}

std::ostream& operator<<(std::ostream& stream, const OperatorPerformanceData& performance_data) {
  performance_data.output_to_stream(stream);
  return stream;
}

std::string OperatorPerformanceData::to_string(DescriptionMode description_mode) const {
  std::string result = format_duration(std::chrono::duration_cast<std::chrono::nanoseconds>(walltime));
  if (input_rows_left != std::numeric_limits<size_t>::max())
    result += std::string(" / Input Left: ") + std::to_string(input_rows_left);
  if (input_rows_right != std::numeric_limits<size_t>::max())
    result += " / Input Right: " + std::to_string(input_rows_right);
  if (output_rows != std::numeric_limits<size_t>::max()) result += " / Output rows: " + std::to_string(output_rows);

  return result;
}

}  // namespace opossum
