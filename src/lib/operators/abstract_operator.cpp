#include "abstract_operator.hpp"

#include <memory>

namespace opossum {

AbstractOperator::AbstractOperator(const std::shared_ptr<const AbstractOperator> left,
                                   const std::shared_ptr<const AbstractOperator> right)
    : _input_left(left), _input_right(right) {}

void AbstractOperator::execute(const TransactionContext *context) { _output = on_execute(context); }

// returns the result of the operator
std::shared_ptr<const Table> AbstractOperator::get_output() const { return _output; }

std::shared_ptr<const Table> AbstractOperator::input_table_left() const { return _input_left->get_output(); }

std::shared_ptr<const Table> AbstractOperator::input_table_right() const { return _input_right->get_output(); }

}  // namespace opossum
