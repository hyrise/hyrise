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

PredicateNode::PredicateNode(const LQPColumnOrigin& column_origin, const ScanType scan_type,
                             const AllParameterVariant& value, const std::optional<AllTypeVariant>& value2)
    : AbstractLQPNode(LQPNodeType::Predicate),
      _column_origin(column_origin),
      _scan_type(scan_type),
      _value(value),
      _value2(value2) {}

std::shared_ptr<AbstractLQPNode> PredicateNode::_deep_copy_impl(const std::shared_ptr<AbstractLQPNode>& left_child, const std::shared_ptr<AbstractLQPNode>& right_child) const {
  DebugAssert(this->left_child(), "Can't copy without child");
  return std::make_shared<PredicateNode>(this->left_child()->clone_column_origin(_column_origin, left_child), _scan_type, _value, _value2);
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

  std::string left_operand_desc = _column_origin.get_verbose_name();
  std::string middle_operand_desc;

  if (_value.type() == typeid(ColumnID)) {
    middle_operand_desc = get_verbose_column_name(boost::get<ColumnID>(_value));
  } else {
    middle_operand_desc = boost::lexical_cast<std::string>(_value);
  }

  std::ostringstream desc;

  desc << "[Predicate] " << left_operand_desc << " " << scan_type_to_string.left.at(_scan_type);
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

const LQPColumnOrigin& PredicateNode::column_origin() const { return _column_origin; }

ScanType PredicateNode::scan_type() const { return _scan_type; }

const AllParameterVariant& PredicateNode::value() const { return _value; }

const std::optional<AllTypeVariant>& PredicateNode::value2() const { return _value2; }

std::shared_ptr<TableStatistics> PredicateNode::derive_statistics_from(
    const std::shared_ptr<AbstractLQPNode>& left_child, const std::shared_ptr<AbstractLQPNode>& right_child) const {
  DebugAssert(left_child && !right_child, "PredicateNode need left_child and no right_child");

  // If value references a Column, we have to resolve its ColumnID (same as for _column_origin below)
  auto value = _value;
  if (value.type() == typeid(LQPColumnOrigin)) {
    // Doing just `value = boost::get<LQPColumnOrigin>(value)` triggers a compiler warning in GCC release builds about
    // the assigned value being uninitialized. There seems to be no reason for this and this way seems to be fine... :(
    value = static_cast<ColumnID::base_type>(get_output_column_id_by_column_origin(boost::get<LQPColumnOrigin>(value)));
  }

  return left_child->get_statistics()->predicate_statistics(get_output_column_id_by_column_origin(_column_origin),
                                                            _scan_type, value, _value2);
}

}  // namespace opossum
