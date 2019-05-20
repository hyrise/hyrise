#pragma once

#include <chrono>
#include <iostream>
#include <string>

#include "types.hpp"

namespace opossum {

// For an example on how this can be extended on a per-operator basis, see JoinIndex

struct OperatorPerformanceData : public Noncopyable {
  virtual ~OperatorPerformanceData() = default;

  std::chrono::nanoseconds walltime{0};
  size_t input_rows_left{std::numeric_limits<size_t>::max()};
  size_t input_rows_right{std::numeric_limits<size_t>::max()};
  size_t output_rows{std::numeric_limits<size_t>::max()};
  std::time_t timestamp;

  virtual void output_to_stream(std::ostream& stream,
                                DescriptionMode description_mode = DescriptionMode::SingleLine) const;

  std::string to_string(DescriptionMode description_mode) const;
};

std::ostream& operator<<(std::ostream& stream, const OperatorPerformanceData& performance_data);

}  // namespace opossum
