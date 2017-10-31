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

PredicateNode::PredicateNode(const ColumnID column_id, const ScanType scan_type, const AllParameterVariant& value,
                             const std::optional<AllTypeVariant>& value2)
    : AbstractASTNode(ASTNodeType::Predicate),
      _column_id(column_id),
      _scan_type(scan_type),
      _value(value),
      _value2(value2) {}

std::string PredicateNode::description() const {
  std::string left_operand_name = get_verbose_column_name(_column_id);
  std::string right_a_operand_name;

  if (_value.type() == typeid(ColumnID)) {
    right_a_operand_name = get_verbose_column_name(boost::get<ColumnID>(_value));
  } else {
    right_a_operand_name = boost::lexical_cast<std::string>(_value);
  }

  std::ostringstream desc;

  desc << "[Predicate] " << left_operand_name << " " << scan_type_to_string.left.at(_scan_type);
  desc << " " << right_a_operand_name << "";
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

const ColumnID PredicateNode::column_id() const { return _column_id; }

ScanType PredicateNode::scan_type() const { return _scan_type; }

const AllParameterVariant& PredicateNode::value() const { return _value; }

const std::optional<AllTypeVariant>& PredicateNode::value2() const { return _value2; }

std::shared_ptr<TableStatistics> PredicateNode::derive_statistics_from(
    const std::shared_ptr<AbstractASTNode>& left_child, const std::shared_ptr<AbstractASTNode>& right_child) const {
  DebugAssert(left_child && !right_child, "PredicateNode need left_child and no right_child");
  return left_child->get_statistics()->predicate_statistics(_column_id, _scan_type, _value, _value2);
}

void PredicateNode::map_column_ids(const ColumnIDMapping& column_id_mapping,
                                   const std::optional<ASTChildSide>& caller_child_side) {
  _column_id = column_id_mapping[_column_id];

  if (_value.type() == typeid(ColumnID)) {
    _value = column_id_mapping[boost::get<ColumnID>(_value)];
  }

  _propagate_column_id_mapping_to_parent(column_id_mapping);
}

}  // namespace opossum
