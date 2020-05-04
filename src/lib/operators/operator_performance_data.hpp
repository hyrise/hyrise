#pragma once

#include <chrono>
#include <iostream>
#include <string>

#include "types.hpp"

namespace opossum {
/**
  * General execution information is stored in OperatorPerformanceData through AbstractOperator::execute().
  * Further, operators can store additional execution information by inheriting from OperatorPerformanceData (e.g.,
  * JoinIndex) or StepOperatorPerformanceData (when operator steps shall be tracked, e.g., HashJoin).
  * Only used in PQP Visualize and plugins right now (analyze PQP).
  */
struct OperatorPerformanceData : public Noncopyable {
  virtual ~OperatorPerformanceData() = default;

  enum class NoStages { };

  bool executed{false};
  std::chrono::nanoseconds walltime{0};

  // Some operators do not return a table (e.g., Insert).
  // Note: The operator returning an empty table will be expressed as has_output == true, output_row_count == 0
  bool has_output{false};
  uint64_t output_row_count{0};
  uint64_t output_chunk_count{0};

  virtual void output_to_stream(std::ostream& stream,
                                DescriptionMode description_mode = DescriptionMode::SingleLine) const;
};

struct StepOperatorPerformanceData : public OperatorPerformanceData {
  StepOperatorPerformanceData() : OperatorPerformanceData{} {}

  std::array<std::chrono::nanoseconds, 10> step_runtimes;

  std::chrono::nanoseconds get_step_runtime(const size_t step) const { return step_runtimes[step]; }

  virtual void output_to_stream(std::ostream& stream,
                                DescriptionMode description_mode = DescriptionMode::SingleLine) const;
};

std::ostream& operator<<(std::ostream& stream, const OperatorPerformanceData& performance_data);
std::ostream& operator<<(std::ostream& stream, const StepOperatorPerformanceData& performance_data);

}  // namespace opossum
