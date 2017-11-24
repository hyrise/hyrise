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

}  // namespace opossum

namespace tpcc {

void execute_tasks_with_context(std::vector<std::shared_ptr<opossum::OperatorTask>>& tasks,
                                const std::shared_ptr<opossum::TransactionContext>& t_context);

template <typename T>
std::shared_ptr<opossum::ValueColumn<T>> create_single_value_column(T value) {
  tbb::concurrent_vector<T> column;
  column.push_back(value);

  return std::make_shared<opossum::ValueColumn<T>>(std::move(column));
}

}  // namespace tpcc
