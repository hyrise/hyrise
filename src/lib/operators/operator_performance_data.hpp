#pragma once

#include <chrono>
#include <iostream>
#include <string>

#include "types.hpp"

namespace opossum {

// For an example on how this can be extended on a per-operator basis, see JoinIndex

struct OperatorPerformanceData : public Noncopyable {
  virtual ~OperatorPerformanceData() = default;

  bool executed{false};
  std::chrono::nanoseconds walltime{0};
  uint64_t output_row_count{0};
  uint64_t output_chunk_count{0};

  virtual void output_to_stream(std::ostream& stream,
                                DescriptionMode description_mode = DescriptionMode::SingleLine) const;
};

std::ostream& operator<<(std::ostream& stream, const OperatorPerformanceData& performance_data);

}  // namespace opossum
