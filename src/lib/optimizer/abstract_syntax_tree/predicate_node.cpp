#include "predicate_node.hpp"

#include <memory>
#include <sstream>
#include <string>

#include "common.hpp"
#include "constant_mappings.hpp"
#include "optimizer/table_statistics.hpp"
#include "utils/assert.hpp"

namespace opossum {

PredicateNode::PredicateNode(const std::string& column_name, ScanType scan_type, const AllParameterVariant value,
                             const optional<AllTypeVariant> value2)
    : AbstractASTNode(ASTNodeType::Predicate),
      _column_name(column_name),
      _scan_type(scan_type),
      _value(value),
      _value2(value2) {}

std::string PredicateNode::description() const {
  std::ostringstream desc;

  desc << "Predicate: [" << _column_name << "] [" << scan_type_to_string.at(_scan_type) << "]";

  return desc.str();
}

const std::string& PredicateNode::column_name() const { return _column_name; }

ScanType PredicateNode::scan_type() const { return _scan_type; }

const AllParameterVariant& PredicateNode::value() const { return _value; }

const optional<AllTypeVariant>& PredicateNode::value2() const { return _value2; }

const std::shared_ptr<TableStatistics> PredicateNode::create_statistics() const {
  Assert(static_cast<bool>(left_child()), "Predicate node needs left input");

  return left_child()->get_or_create_statistics()->predicate_statistics(_column_name, _scan_type, _value, _value2);
}

}  // namespace opossum
