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
  std::ostringstream desc;

  desc << "Predicate: Col #" << _column_id << " " << scan_type_to_string.left.at(_scan_type);
  desc << " '" << _value << "'";
  if (_value2) {
    desc << " '" << (*_value2) << "";
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

}  // namespace opossum
