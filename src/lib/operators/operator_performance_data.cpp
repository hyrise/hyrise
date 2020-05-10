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

void StepOperatorPerformanceData::output_to_stream(std::ostream& stream, DescriptionMode description_mode) const {
  // As we do not know the number of stages of the operator at this point (and we do not know if there might be
  // skipped stage in between stages, we search backwards for the first non-zero value and assume that the position
  // index equals the number of stages of the operator.
  const auto step_count =
      std::distance(std::find_if(step_runtimes.crbegin(), step_runtimes.crend(),
                                 [](const auto& value) { return value != std::chrono::nanoseconds::zero(); }),
                    step_runtimes.crend());

  stream << "Stage runtimes: ";
  for (auto step = 0; step < step_count; ++step) {
    if (step > 0) stream << ", ";
    stream << format_duration(step_runtimes[step]);
  }
  stream << ". ";
}

std::ostream& operator<<(std::ostream& stream, const StepOperatorPerformanceData& performance_data) {
  performance_data.output_to_stream(stream);
  return stream;
}

}  // namespace opossum
