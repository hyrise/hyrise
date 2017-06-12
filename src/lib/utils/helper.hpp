#pragma once

#include <iostream>
#include <memory>
#include <vector>

#include "operators/print.hpp"

namespace opossum {

class Table;
class TransactionContext;
class AbstractOperator;

void set_transaction_context_for_operators(const std::shared_ptr<TransactionContext> t_context,
                                           const std::vector<std::shared_ptr<AbstractOperator>> operators);

void print_table(const std::shared_ptr<const Table> table, PrintMode mode = PrintMode::IgnoreEmptyChunks,
                 std::ostream& out = std::cout);

}