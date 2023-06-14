#include <utility>

#include "storage/value_segment.hpp"
#include "window_function_evaluator.hpp"

namespace hyrise {

const std::string& WindowFunctionEvaluator::name() const {
  static const auto name = std::string{"WindowFunctionEvaluator"};
  return name;
}

std::shared_ptr<const Table> WindowFunctionEvaluator::_on_execute() {
  auto partitioned_data = partition_and_sort();

  // TODO: need to know output type of window function
  using T = int;

  std::vector<ValueSegment<T>> result_segments(left_input_table()->chunk_count());
  compute_window_function(partitioned_data, [&](RowID row_id, AllTypeVariant computed_value) {
    result_segments[row_id.chunk_id][row_id.chunk_offset] = std::move(computed_value);
  });

  return annotate_input_table(std::move(result_segments));
}

WindowFunctionEvaluator::PartitionedData WindowFunctionEvaluator::partition_and_sort() const {
  (void)left_input_table();
  Fail("unimplemented");
}

void WindowFunctionEvaluator::compute_window_function(const PartitionedData& partitioned_data,
                                                      auto&& emit_computed_value) const {
  (void)partitioned_data;
  (void)emit_computed_value;
  Fail("unimplemented");
}

template <typename T>
std::shared_ptr<const Table> WindowFunctionEvaluator::annotate_input_table(
    std::vector<ValueSegment<T>> segments_for_output_column) const {
  (void)segments_for_output_column;
  Fail("Unimplemented");
}

}  // namespace hyrise
