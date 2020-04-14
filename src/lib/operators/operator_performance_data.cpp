#include "operator_performance_data.hpp"

#include <string>

#include "utils/format_duration.hpp"

namespace opossum {

void OperatorPerformanceData::output_to_stream(std::ostream& stream, DescriptionMode description_mode) const {
  if (!executed) {
    stream << "not executed";
    return;
  }

  if (!has_output) {
    stream << "executed, but no output";
    return;
  }

  stream << output_row_count << " row(s) in ";
  stream << output_chunk_count << " chunk(s), ";
  stream << format_duration(std::chrono::duration_cast<std::chrono::nanoseconds>(walltime));
}

std::ostream& operator<<(std::ostream& stream, const OperatorPerformanceData& performance_data) {
  performance_data.output_to_stream(stream);
  return stream;
}

}  // namespace opossum
