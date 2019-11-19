#pragma once

#include <chrono>
#include <iostream>
#include <string>

#include <boost/container/small_vector.hpp>

#include "types.hpp"

namespace opossum {

// For an example on how this can be extended on a per-operator basis, see JoinIndex

struct OperatorPerformanceData : public Noncopyable {
  virtual ~OperatorPerformanceData() = default;

  std::chrono::nanoseconds walltime{0};

  std::optional<size_t> input_row_count_left;
  std::optional<size_t> input_row_count_right;
  size_t output_row_count{0};

  virtual void output_to_stream(std::ostream& stream,
                                DescriptionMode description_mode = DescriptionMode::SingleLine) const;
};

// Small vector with a size of 4 is chosen as the operator with the currently most stages has six stages.
struct StagedOperatorPerformanceData : public OperatorPerformanceData {
  StagedOperatorPerformanceData() : OperatorPerformanceData{} {}
  boost::container::small_vector<std::chrono::nanoseconds, 6> stage_runtimes;

  std::chrono::nanoseconds get_stage_runtime(const uint8_t stage) const {
  	return stage_runtimes[stage];
  }
};

std::ostream& operator<<(std::ostream& stream, const OperatorPerformanceData& performance_data);

}  // namespace opossum
