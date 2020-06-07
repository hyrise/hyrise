#pragma once

#include <chrono>
#include <iostream>
#include <string>

#include <magic_enum.hpp>

#include "types.hpp"
#include "utils/format_duration.hpp"

namespace opossum {
/**
TODO
  * General execution information is stored in OperatorPerformanceData through AbstractOperator::execute().
  * Further, operators can store additional execution information by inheriting from OperatorPerformanceData (e.g.,
  * JoinIndex) or StepOperatorPerformanceData (when operator steps shall be tracked, e.g., HashJoin).
  * Only used in PQP Visualize and plugins right now (analyze PQP).
  */
struct AbstractOperatorPerformanceData : public Noncopyable {
  enum class NoSteps {
    Invalid  // Needed by magic_enum for enum_count
  };

  virtual ~AbstractOperatorPerformanceData() = default;

  virtual void output_to_stream(std::ostream& stream, DescriptionMode description_mode) const = 0;

  bool executed{false};
  std::chrono::nanoseconds walltime{0};

  // Some operators do not return a table (e.g., Insert).
  // Note: The operator returning an empty table will be expressed as has_output == true, output_row_count == 0
  bool has_output{false};
  uint64_t output_row_count{0};
  uint64_t output_chunk_count{0};
};

template <typename Steps>
struct OperatorPerformanceData : public AbstractOperatorPerformanceData {
  void output_to_stream(std::ostream& stream, DescriptionMode description_mode) const {
    if (!executed) {
      stream << "Not executed.";
      return;
    }

    if (!has_output) {
      stream << "Executed, but no output.";
      return;
    }

    stream << "Output: " << output_row_count << " row" << (output_row_count > 1 ? "s" : "") << " in "
           << output_chunk_count << " chunk" << (output_chunk_count > 1 ? "s" : "") << ", "
           << format_duration(std::chrono::duration_cast<std::chrono::nanoseconds>(walltime)) << ".";

    if constexpr (!std::is_same_v<Steps, NoSteps>) {
      static_assert(magic_enum::enum_count<Steps>() <= sizeof(step_runtimes), "Too many steps.");
      const auto separator = description_mode == DescriptionMode::SingleLine ? " " : "\n";
      stream << separator << "Operator step runtimes:" << separator;
      for (auto step_index = size_t{0}; step_index < magic_enum::enum_count<Steps>(); ++step_index) {
        if (step_index > 0) {
          stream << (description_mode == DescriptionMode::SingleLine ? "," : "\n");
        }
        stream << " " << magic_enum::enum_name(static_cast<Steps>(step_index)) << " "
               << format_duration(step_runtimes[step_index]);
      }
      stream << ".";
    }
  }

  std::chrono::nanoseconds get_step_runtime(const Steps step) const {
    DebugAssert(magic_enum::enum_integer(step) < magic_enum::enum_count<Steps>(), "Step index is too large.");
    return step_runtimes[static_cast<size_t>(step)];
  }

  void set_step_runtime(const Steps step, const std::chrono::nanoseconds duration) {
    step_runtimes[static_cast<size_t>(step)] = duration;
  }

  std::array<std::chrono::nanoseconds, magic_enum::enum_count<Steps>()> step_runtimes{};
};

std::ostream& operator<<(std::ostream& stream, const AbstractOperatorPerformanceData& performance_data);

}  // namespace opossum
