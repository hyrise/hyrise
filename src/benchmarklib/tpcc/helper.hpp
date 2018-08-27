#pragma once

#include <tbb/tbb.h>

#include <memory>
#include <utility>
#include <vector>

namespace opossum {

class OperatorTask;
class TransactionContext;

template <typename>
class ValueColumn;

void execute_tasks_with_context(std::vector<std::shared_ptr<OperatorTask>>& tasks,
                                const std::shared_ptr<TransactionContext>& transaction_context);

template <typename T>
std::shared_ptr<ValueColumn<T>> create_single_value_column(T value) {
  tbb::concurrent_vector<T> column;
  column.push_back(value);

  return std::make_shared<ValueColumn<T>>(std::move(column));
}

}  // namespace opossum
