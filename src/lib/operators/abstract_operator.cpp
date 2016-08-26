#include "abstract_operator.hpp"

#include <memory>

namespace opossum {

AbstractOperator::AbstractOperator(const std::shared_ptr<AbstractOperator> left,
                                   const std::shared_ptr<AbstractOperator> right)
    : _input_left(left ? left->get_output() : nullptr), _input_right(right ? right->get_output() : nullptr) {}
}  // namespace opossum
