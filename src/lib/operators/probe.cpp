#include "probe.hpp"

#include <algorithm>
#include <cstddef>
#include <memory>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "all_type_variant.hpp"
#include "expression/abstract_expression.hpp"
#include "expression/evaluation/expression_evaluator.hpp"
#include "expression/expression_utils.hpp"
#include "operators/abstract_operator.hpp"
#include "operators/abstract_read_only_operator.hpp"
#include "resolve_type.hpp"
#include "storage/chunk.hpp"
#include "storage/pos_lists/row_id_pos_list.hpp"
#include "storage/reference_segment.hpp"
#include "storage/table.hpp"
#include "types.hpp"
#include "utils/assert.hpp"

namespace hyrise {

Probe::Probe(const std::shared_ptr<const AbstractOperator>& input_operator, const ColumnID column_id)
    : AbstractReadOnlyOperator(OperatorType::Probe, input_operator), _column_id{column_id} {}

const std::string& Probe::name() const {
  static const auto name = std::string{"Probe"};
  return name;
}

ColumnID Probe::column_id() const {
  return _column_id;
}

std::shared_ptr<AbstractOperator> Probe::_on_deep_copy(
    const std::shared_ptr<AbstractOperator>& copied_left_input,
    const std::shared_ptr<AbstractOperator>& /*copied_right_input*/,
    std::unordered_map<const AbstractOperator*, std::shared_ptr<AbstractOperator>>& copied_ops) const {
  return std::make_shared<Probe>(copied_left_input, _column_id);
}

std::shared_ptr<const Table> Probe::_on_execute() {
  return left_input_table();
}

void Probe::_on_set_parameters(const std::unordered_map<ParameterID, AllTypeVariant>& parameters) {}

}  // namespace hyrise
