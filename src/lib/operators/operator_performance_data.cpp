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

void StagedOperatorPerformanceData::output_to_stream(std::ostream& stream, DescriptionMode description_mode) const {
  OperatorPerformanceData::output_to_stream(stream, description_mode);

	// As we do not know the number of stages of the operator at this point, we search for the first zero value and
	// assume that the position index equals the number of stages of the operator.
	auto stage_count = std::distance(
      stage_runtimes.cbegin(), std::find_if(stage_runtimes.cbegin(), stage_runtimes.cend(),
                                         [](const auto& value) { return value == std::chrono::nanoseconds::zero(); }));

  const auto separator = description_mode == DescriptionMode::MultiLine ? "\n" : " ";
  stream << separator << "Stages:" << separator;
	for (auto stage = 0; stage < stage_count; ++stage) {
	  stream << format_duration(stage_runtimes[stage]);
	  if (stage < stage_count - 1) stream << " | ";
	}
  stream << separator;
}

std::ostream& operator<<(std::ostream& stream, const StagedOperatorPerformanceData& performance_data) {
  performance_data.output_to_stream(stream);
  return stream;
}

}  // namespace opossum
