#include "predicate_node.hpp"

#include <memory>
#include <optional>
#include <sstream>
#include <string>

#include "constant_mappings.hpp"
#include "optimizer/table_statistics.hpp"
#include "types.hpp"
#include "utils/assert.hpp"

namespace opossum {

PredicateNode::PredicateNode(const LQPColumnReference& column_reference, const PredicateCondition predicate_condition,
                             const AllParameterVariant& value, const std::optional<AllTypeVariant>& value2)
    : AbstractLQPNode(LQPNodeType::Predicate),
      _column_reference(column_reference),
      _predicate_condition(predicate_condition),
      _value(value),
      _value2(value2) {}

std::shared_ptr<AbstractLQPNode> PredicateNode::_deep_copy_impl(
    const std::shared_ptr<AbstractLQPNode>& copied_left_input,
    const std::shared_ptr<AbstractLQPNode>& copied_right_input) const {
  DebugAssert(left_input(), "Can't copy without input");

  auto value = _value;
  if (is_lqp_column_reference(_value)) {
    value =
        adapt_column_reference_to_different_lqp(boost::get<LQPColumnReference>(value), left_input(), copied_left_input);
  }
  return PredicateNode::make(
      adapt_column_reference_to_different_lqp(_column_reference, left_input(), copied_left_input), _predicate_condition,
      value, _value2);
}

std::string PredicateNode::description() const {
  /**
   * " a BETWEEN 5 AND c"
   *  (0)       (1)   (2)
   * " b >=     13"
   *
   * (0) left operand
   * (1) middle operand
   * (2) right operand (only for BETWEEN)
   */

  std::string left_operand_desc = _column_reference.description();
  std::string middle_operand_desc;

  if (_value.type() == typeid(ColumnID)) {
    middle_operand_desc = get_verbose_column_name(boost::get<ColumnID>(_value));
  } else {
    middle_operand_desc = boost::lexical_cast<std::string>(_value);
  }

  std::ostringstream desc;

  desc << "[Predicate] " << left_operand_desc << " " << predicate_condition_to_string.left.at(_predicate_condition);
  desc << " " << middle_operand_desc << "";
  if (_value2) {
    desc << " AND ";
    if (_value2->type() == typeid(std::string)) {
      desc << "'" << *_value2 << "'";
    } else {
      desc << (*_value2);
    }
  }

  return desc.str();
}

const LQPColumnReference& PredicateNode::column_reference() const { return _column_reference; }

PredicateCondition PredicateNode::predicate_condition() const { return _predicate_condition; }

const AllParameterVariant& PredicateNode::value() const { return _value; }

const std::optional<AllTypeVariant>& PredicateNode::value2() const { return _value2; }

ScanType PredicateNode::scan_type() const { return _scan_type; }

void PredicateNode::set_scan_type(ScanType scan_type) { _scan_type = scan_type; }

std::shared_ptr<TableStatistics> PredicateNode::derive_statistics_from(
    const std::shared_ptr<AbstractLQPNode>& left_input, const std::shared_ptr<AbstractLQPNode>& right_input) const {
  DebugAssert(left_input && !right_input, "PredicateNode need left_input and no right_input");

  // If value references a Column, we have to resolve its ColumnID (same as for _column_reference below)
  auto value = _value;
  if (is_lqp_column_reference(value)) {
    // Doing just `value = boost::get<LQPColumnReference>(value)` triggers a compiler warning in GCC release builds
    // about the assigned value being uninitialized. There seems to be no reason for this and this way seems to be
    // fine... :(
    value = static_cast<ColumnID::base_type>(left_input->get_output_column_id(boost::get<LQPColumnReference>(value)));
  }

  return left_input->get_statistics()->predicate_statistics(left_input->get_output_column_id(_column_reference),
                                                            _predicate_condition, value, _value2);
}

bool PredicateNode::shallow_equals(const AbstractLQPNode& rhs) const {
  Assert(rhs.type() == type(), "Can only compare nodes of the same type()");
  const auto& predicate_node = static_cast<const PredicateNode&>(rhs);

  if (!_equals(*this, _column_reference, predicate_node, predicate_node._column_reference)) return false;
  if (_predicate_condition != predicate_node._predicate_condition) return false;
  if (is_lqp_column_reference(_value) != is_lqp_column_reference(predicate_node._value)) return false;
  if (is_lqp_column_reference(_value)) {
    if (!_equals(*this, boost::get<LQPColumnReference>(_value), predicate_node,
                 boost::get<LQPColumnReference>(predicate_node._value)))
      return false;
  } else {
    if (!all_parameter_variant_near(_value, predicate_node._value)) return false;
  }

  if (_value2.has_value() != predicate_node._value2.has_value()) return false;
  if (!_value2.has_value() && !predicate_node._value2.has_value()) return true;

  return all_type_variant_near(*_value2, *predicate_node._value2);
}

}  // namespace opossum
