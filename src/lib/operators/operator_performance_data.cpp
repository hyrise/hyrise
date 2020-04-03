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
  }

  stream << output_row_count << " row(s) in ";
  stream << output_chunk_count << " chunk(s), ";
  stream << format_duration(std::chrono::duration_cast<std::chrono::nanoseconds>(walltime));
}

std::ostream& operator<<(std::ostream& stream, const OperatorPerformanceData& performance_data) {
  performance_data.output_to_stream(stream);
  return stream;
}

void StagedOperatorPerformanceData::output_to_stream(std::ostream& stream, DescriptionMode description_mode) const {
  // As we do not know the number of stages of the operator at this point (and we do not know if there might be
  // skipped stage in between stages, we search backwards for the first non-zero value and assume that the position
  // index equals the number of stages of the operator.
  const auto stage_count =
      std::distance(std::find_if(stage_runtimes.crbegin(), stage_runtimes.crend(),
                                 [](const auto& value) { return value != std::chrono::nanoseconds::zero(); }),
                    stage_runtimes.crend());

  stream << "Stage runtimes: ";
  for (auto stage = 0; stage < stage_count; ++stage) {
    if (stage > 0) stream << ", ";
    stream << format_duration(stage_runtimes[stage]);
  }
  stream << ". ";
}

std::ostream& operator<<(std::ostream& stream, const StagedOperatorPerformanceData& performance_data) {
  performance_data.output_to_stream(stream);
  return stream;
}

}  // namespace opossum
