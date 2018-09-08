#pragma once

#include <tbb/tbb.h>

#include <memory>
#include <utility>
#include <vector>

namespace opossum {

class OperatorTask;
class TransactionContext;

template <typename>
class ValueSegment;

void execute_tasks_with_context(std::vector<std::shared_ptr<OperatorTask>>& tasks,
                                const std::shared_ptr<TransactionContext>& transaction_context);

template <typename T>
std::shared_ptr<ValueSegment<T>> create_single_value_segment(T value) {
  tbb::concurrent_vector<T> vector;
  vector.push_back(value);

  return std::make_shared<ValueSegment<T>>(std::move(vector));
}

}  // namespace opossum
