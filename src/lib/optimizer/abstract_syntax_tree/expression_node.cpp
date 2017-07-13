#include "expression_node.hpp"

#include <memory>
#include <sstream>
#include <string>
#include <vector>

#include "type_cast.hpp"
#include "all_type_variant.hpp"
#include "constant_mappings.hpp"
#include "common.hpp"
#include "constant_mappings.hpp"
#include "utils/assert.hpp"

namespace opossum {

const std::unordered_map<ExpressionType, std::string> expression_type_to_operator = {
  {ExpressionType::Plus, "+"},     {ExpressionType::Minus, "-"},
  {ExpressionType::Asterisk, "*"}, {ExpressionType::Slash, "/"},
};

ExpressionNode::ExpressionNode(const ExpressionType type)
    : AbstractExpressionNode(type),
      _value(NULL_VALUE) /*, _value2(NULL_VALUE)*/,
      _expression_list({}),
      _name("-"),
      _table("-") {}

ExpressionNode::ExpressionNode(const ExpressionType type, const std::string &table_name, const std::string &column_name)
    : AbstractExpressionNode(type),
      _value(NULL_VALUE) /*, _value2(NULL_VALUE)*/,
      _expression_list({}),
      _name(column_name),
      _table(table_name) {}

ExpressionNode::ExpressionNode(const ExpressionType type, const AllTypeVariant value /*, const AllTypeVariant value2*/)
    : AbstractExpressionNode(type),
      _value(value) /*, _value2(value2)*/,
      _expression_list({}),
      _name("-"),
      _table("-") {}

ExpressionNode::ExpressionNode(const ExpressionType type, const std::string &function_name,
                               std::shared_ptr<std::vector<std::shared_ptr<ExpressionNode>>> expression_list)
    : AbstractExpressionNode(type),
      _value(NULL_VALUE) /*, _value2(NULL_VALUE)*/,
      _expression_list(expression_list),
      _name(function_name),
      _table("-") {}

AggregateFunction ExpressionNode::as_aggregate_function() const {
  Fail("Can't do this right now");
  return AggregateFunction::Avg;
}

std::string ExpressionNode::description() const {
  std::ostringstream desc;

  desc << "Expression (" << expression_type_to_string.at(_type) << ", " << value() /*<< ", " << value2()*/ << ", "
       << table_name() << ", " << column_name() << ")";

  return desc.str();
}

bool ExpressionNode::is_arithmetic() const {
  return _type == ExpressionType::Minus || _type == ExpressionType::Plus
         || _type == ExpressionType::Asterisk || _type == ExpressionType::Slash;
}

bool ExpressionNode::is_operand() const {
  return
    _type == ExpressionType::Literal ||
    _type == ExpressionType::ColumnReference;
}

const std::string &ExpressionNode::table_name() const { return _table; }

const std::string &ExpressionNode::name() const { return _name; }

const std::string &ExpressionNode::column_name() const { return _name; }

const AllTypeVariant ExpressionNode::value() const { return _value; }

// const AllTypeVariant ExpressionNode::value2() const {
//  return _value2;
//}

std::string ExpressionNode::to_expression_string() const {
  if (is_operand()) {
    return type_cast<std::string>(_value);
  } else if (is_arithmetic()) { // TODO(mp) Should be is_operator() to also support "=", ...
    Assert(static_cast<bool>(left()) && static_cast<bool>(right()), "Operator needs both operands");
    auto left_expression_node = std::static_pointer_cast<ExpressionNode>(left());
    auto right_expression_node = std::static_pointer_cast<ExpressionNode>(right());
    Assert(static_cast<bool>(left_expression_node) &&
             static_cast<bool>(right_expression_node), "Operator needs both operands to be expressions");

    return left_expression_node->to_expression_string() + expression_type_to_operator.at(_type) +
      right_expression_node->to_expression_string();
  } else {
    Fail("To generate expression string, ExpressionNodes need to be operators or operands");
  }

  return "";
}

const std::shared_ptr<std::vector<std::shared_ptr<ExpressionNode>>> &ExpressionNode::expression_list() const {
  return _expression_list;
}

}  // namespace opossum
