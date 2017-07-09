#pragma once

#include <tbb/tbb.h>

#include <iostream>
#include <memory>
#include <utility>
#include <vector>

#include "operators/print.hpp"

namespace opossum {

class Table;
class TransactionContext;
class AbstractOperator;
class OperatorTask;
template <typename>
class ValueColumn;

void print_table(const std::shared_ptr<const Table> table, uint32_t flags = PrintIgnoreEmptyChunks,
                 std::ostream& out = std::cout);

template <typename T>
std::shared_ptr<ValueColumn<T>> create_single_value_column(T value) {
  tbb::concurrent_vector<T> column;
  column.push_back(value);

  return std::make_shared<ValueColumn<T>>(std::move(column));
}

}  // namespace opossum
