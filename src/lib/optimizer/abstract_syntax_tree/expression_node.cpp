#include "expression_node.hpp"

#include <sstream>
#include <string>

#include "all_type_variant.hpp"

#include "common.hpp"

namespace opossum {

ExpressionNode::ExpressionNode(const ExpressionType type)
  : AbstractNode(NodeType::Expression), _type(type) {}

const std::string ExpressionNode::description() const {
  std::ostringstream desc;

  desc << "Expression (" << _type << ")";

  return desc.str();
}

const std::string &ExpressionNode::table_name() const {
  return _table;
}

const std::string &ExpressionNode::column_name() const {
  return _name;
}

const AllTypeVariant ExpressionNode::value() const {
  return _value;
}

const AllTypeVariant ExpressionNode::value2() const {
  return _value2;
}

const ExpressionType ExpressionNode::expression_type() const {
  return _type;
}


}  // namespace opossum
