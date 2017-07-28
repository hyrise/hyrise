#include "predicate_node.hpp"

#include <memory>
#include <string>

#include "common.hpp"
#include "constant_mappings.hpp"

namespace opossum {

PredicateNode::PredicateNode(const std::string& column_name, const std::shared_ptr<ExpressionNode> predicate,
                             const ScanType scan_type, const AllParameterVariant value,
                             const optional<AllTypeVariant> value2)
    : AbstractASTNode(ASTNodeType::Predicate),
      _column_name(column_name),
      _predicate(predicate),
      _scan_type(scan_type),
      _value(value),
      _value2(value2) {}

std::string PredicateNode::description() const {
  std::ostringstream desc;

  // TODO(anyone): correctly print _predicate as soon as it is fully used
  desc << "Predicate: [" << _column_name << "] [" << scan_type_to_string.left.at(_scan_type) << "] ";

  return desc.str();
}

const std::string& PredicateNode::column_name() const { return _column_name; }

const std::shared_ptr<ExpressionNode> PredicateNode::predicate() const { return _predicate; }

void PredicateNode::set_predicate(const std::shared_ptr<ExpressionNode> predicate) { _predicate = predicate; }

ScanType PredicateNode::scan_type() const { return _scan_type; }

const AllParameterVariant& PredicateNode::value() const { return _value; }

const optional<AllTypeVariant>& PredicateNode::value2() const { return _value2; }

}  // namespace opossum
