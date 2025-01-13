#include "reduce.hpp"

namespace hyrise {

Reduce::Reduce(const std::shared_ptr<const AbstractOperator>& input_relation,
               const std::shared_ptr<const AbstractOperator>& input_filter)
    : AbstractReadOnlyOperator{OperatorType::Reduce, input_relation, input_filter} {}

const std::string& Reduce::name() const {
  static const auto name = std::string{"Reduce"};
  return name;
}

std::shared_ptr<const Table> Reduce::_on_execute() {
  return left_input_table();
}

void Reduce::_on_set_parameters(const std::unordered_map<ParameterID, AllTypeVariant>& parameters) {}

std::shared_ptr<AbstractOperator> Reduce::_on_deep_copy(
    const std::shared_ptr<AbstractOperator>& copied_left_input,
    const std::shared_ptr<AbstractOperator>& /*copied_right_input*/,
    std::unordered_map<const AbstractOperator*, std::shared_ptr<AbstractOperator>>& copied_ops) const {
  return nullptr;
}

}  // namespace hyrise
