#include "expression_node.hpp"

#include <iomanip>
#include <iostream>
#include <memory>
#include <sstream>
#include <string>
#include <unordered_map>
#include <vector>

#include "all_type_variant.hpp"
#include "common.hpp"
#include "constant_mappings.hpp"
#include "type_cast.hpp"
#include "utils/assert.hpp"

namespace opossum {

const std::unordered_map<ExpressionType, std::string> expression_type_to_operator = {
    {ExpressionType::Plus, "+"},
    {ExpressionType::Minus, "-"},
    {ExpressionType::Asterisk, "*"},
    {ExpressionType::Slash, "/"},
};

ExpressionNode::ExpressionNode(const ExpressionType type, const AllTypeVariant value,
                               const std::shared_ptr<std::vector<std::shared_ptr<ExpressionNode>>> expression_list,
                               const std::string &name, const std::string &table)
    : _type(type),
      _value(value) /*, _value2(NULL_VALUE)*/,
      _expression_list(expression_list),
      _name(name),
      _table(table) {}

std::shared_ptr<ExpressionNode> ExpressionNode::create_expression(const ExpressionType type) {
  auto expr_list = std::make_shared<std::vector<std::shared_ptr<ExpressionNode>>>();
  return std::make_shared<ExpressionNode>(type, NULL_VALUE, expr_list, "", "");
}

std::shared_ptr<ExpressionNode> ExpressionNode::create_column_reference(const std::string &table_name,
                                                                        const std::string &column_name) {
  auto expr_list = std::make_shared<std::vector<std::shared_ptr<ExpressionNode>>>();
  return std::make_shared<ExpressionNode>(ExpressionType::ColumnReference, NULL_VALUE, expr_list, column_name,
                                          table_name);
}

std::shared_ptr<ExpressionNode> ExpressionNode::create_literal(const AllTypeVariant value) {
  auto expr_list = std::make_shared<std::vector<std::shared_ptr<ExpressionNode>>>();
  return std::make_shared<ExpressionNode>(ExpressionType::Literal, value, expr_list, "", "");
}

std::shared_ptr<ExpressionNode> ExpressionNode::create_parameter(const AllTypeVariant value) {
  auto expr_list = std::make_shared<std::vector<std::shared_ptr<ExpressionNode>>>();
  return std::make_shared<ExpressionNode>(ExpressionType::Parameter, value, expr_list, "", "");
}

std::shared_ptr<ExpressionNode> ExpressionNode::create_function_reference(
    const std::string &function_name, std::shared_ptr<std::vector<std::shared_ptr<ExpressionNode>>> expression_list) {
  return std::make_shared<ExpressionNode>(ExpressionType::FunctionReference, NULL_VALUE, expression_list, function_name,
                                          "");
}

const std::weak_ptr<ExpressionNode> &ExpressionNode::parent() const { return _parent; }

void ExpressionNode::set_parent(const std::weak_ptr<ExpressionNode> &parent) { _parent = parent; }

const std::shared_ptr<ExpressionNode> &ExpressionNode::left_child() const { return _left_child; }

void ExpressionNode::set_left_child(const std::shared_ptr<ExpressionNode> &left) {
  _left_child = left;
  left->_parent = shared_from_this();
}

const std::shared_ptr<ExpressionNode> &ExpressionNode::right_child() const { return _right_child; }

void ExpressionNode::set_right_child(const std::shared_ptr<ExpressionNode> &right) {
  _right_child = right;
  right->_parent = shared_from_this();
}

const ExpressionType ExpressionNode::type() const { return _type; }

void ExpressionNode::print(const uint8_t level) const {
  std::cout << std::setw(level) << " ";
  std::cout << description() << std::endl;

  if (_left_child) {
    _left_child->print(level + 2);
  }

  if (_right_child) {
    _right_child->print(level + 2);
  }
}

bool ExpressionNode::is_arithmetic() const {
  return _type == ExpressionType::Minus || _type == ExpressionType::Plus || _type == ExpressionType::Asterisk ||
         _type == ExpressionType::Slash;
}

bool ExpressionNode::is_operand() const {
  return _type == ExpressionType::Literal || _type == ExpressionType::ColumnReference;
}

const std::string ExpressionNode::description() const {
  std::ostringstream desc;

  desc << "Expression (" << expression_type_to_string.at(_type) << ", " << value() /*<< ", " << value2()*/ << ", "
       << table_name() << ", " << column_name() << ")";

  return desc.str();
}

const std::string &ExpressionNode::table_name() const { return _table; }

const std::string &ExpressionNode::name() const { return _name; }

const std::string &ExpressionNode::column_name() const { return _name; }

const AllTypeVariant ExpressionNode::value() const { return _value; }

// const AllTypeVariant ExpressionNode::value2() const {
//  return _value2;
//}

std::string ExpressionNode::to_expression_string() const {
  if (_type == ExpressionType::Literal) {
    return type_cast<std::string>(_value);
  } else if (_type == ExpressionType::ColumnReference) {
    return "$" + _name;
  } else if (is_arithmetic()) {
    // TODO(mp) Should be is_operator() to also support "=", ...
    Assert(static_cast<bool>(left_child()) && static_cast<bool>(right_child()), "Operator needs both operands");

    auto left_expression_node = std::static_pointer_cast<ExpressionNode>(left_child());
    auto right_expression_node = std::static_pointer_cast<ExpressionNode>(right_child());
    Assert(static_cast<bool>(left_expression_node) && static_cast<bool>(right_expression_node),
           "Operator needs both operands to be expressions");

    return left_expression_node->to_expression_string() + expression_type_to_operator.at(_type) +
           right_expression_node->to_expression_string();
  } else if (_type == ExpressionType::FunctionReference) {
    return _name + "()";
  } else {
    Fail("To generate expression string, ExpressionNodes need to be operators or operands");
  }

  return "";
}

const std::shared_ptr<std::vector<std::shared_ptr<ExpressionNode>>> &ExpressionNode::expression_list() const {
  return _expression_list;
}

}  // namespace opossum
