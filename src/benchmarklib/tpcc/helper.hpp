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

void execute_tasks_with_context(std::vector<opossum::OperatorTaskSPtr>& tasks,
                                opossum::TransactionContextSPtr t_context);

template <typename T>
opossum::ValueColumnSPtr<T> create_single_value_column(T value) {
  tbb::concurrent_vector<T> column;
  column.push_back(value);

  return std::make_shared<opossum::ValueColumn<T>>(std::move(column));
}

}  // namespace tpcc
