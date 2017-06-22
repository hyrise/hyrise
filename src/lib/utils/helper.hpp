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
template <typename>
class ValueColumn;

void set_transaction_context_for_operators(const std::shared_ptr<TransactionContext> t_context,
                                           const std::vector<std::shared_ptr<AbstractOperator>> operators);

void print_table(const std::shared_ptr<const Table> table, PrintMode mode = PrintMode::IgnoreEmptyChunks,
                 std::ostream& out = std::cout);

template <typename T>
std::shared_ptr<ValueColumn<T>> create_single_value_column(T value) {
  tbb::concurrent_vector<T> column;
  column.push_back(value);

  return std::make_shared<ValueColumn<T>>(std::move(column));
}

}  // namespace opossum
