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

ExpressionNode::ExpressionNode(const ExpressionType type, const AllTypeVariant &value,
                               const std::vector<std::shared_ptr<ExpressionNode>> &expression_list,
                               const std::string &name, const std::string &table, const optional<std::string> &alias)
    : _type(type), _value(value), _expression_list(expression_list), _name(name), _table_name(table), _alias(alias) {}

std::shared_ptr<ExpressionNode> ExpressionNode::create_expression(const ExpressionType type) {
  const std::vector<std::shared_ptr<ExpressionNode>> expr_list;
  return std::make_shared<ExpressionNode>(type, NULL_VALUE, expr_list, "", "");
}

std::shared_ptr<ExpressionNode> ExpressionNode::create_column_reference(const std::string &column_name,
                                                                        const std::string &table_name,
                                                                        const optional<std::string> &alias) {
  const std::vector<std::shared_ptr<ExpressionNode>> expr_list;
  return std::make_shared<ExpressionNode>(ExpressionType::ColumnReference, NULL_VALUE, expr_list, column_name,
                                          table_name, alias);
}

std::shared_ptr<ExpressionNode> ExpressionNode::create_literal(const AllTypeVariant &value,
                                                               const optional<std::string> &alias) {
  const std::vector<std::shared_ptr<ExpressionNode>> expr_list;
  return std::make_shared<ExpressionNode>(ExpressionType::Literal, value, expr_list, "", "", alias);
}

std::shared_ptr<ExpressionNode> ExpressionNode::create_parameter(const AllTypeVariant &value) {
  const std::vector<std::shared_ptr<ExpressionNode>> expr_list;
  return std::make_shared<ExpressionNode>(ExpressionType::Placeholder, value, expr_list, "", "");
}

std::shared_ptr<ExpressionNode> ExpressionNode::create_function_reference(
    const std::string &function_name, const std::vector<std::shared_ptr<ExpressionNode>> &expression_list,
    const optional<std::string> &alias) {
  return std::make_shared<ExpressionNode>(ExpressionType::FunctionReference, NULL_VALUE, expression_list, function_name,
                                          "", alias);
}

std::shared_ptr<ExpressionNode> ExpressionNode::create_binary_operator(ExpressionType type,
                                                                       const std::shared_ptr<ExpressionNode> &left,
                                                                       const std::shared_ptr<ExpressionNode> &right,
                                                                       const optional<std::string> &alias) {
  auto expression = std::make_shared<ExpressionNode>(type, AllTypeVariant(),
                                                     std::vector<std::shared_ptr<ExpressionNode>>(), "", "", alias);
  Assert(expression->is_binary_operator(), "Type is not an operator type");

  expression->set_left_child(left);
  expression->set_right_child(right);

  return expression;
}

std::shared_ptr<ExpressionNode> ExpressionNode::create_select_all() {
  return std::make_shared<ExpressionNode>(ExpressionType::Star, AllTypeVariant(),
                                          std::vector<std::shared_ptr<ExpressionNode>>(), "", "", nullopt);
}

const std::weak_ptr<ExpressionNode> ExpressionNode::parent() const { return _parent; }

void ExpressionNode::clear_parent() { _parent.reset(); }

const std::shared_ptr<ExpressionNode> ExpressionNode::left_child() const { return _left_child; }

void ExpressionNode::set_left_child(const std::shared_ptr<ExpressionNode> &left) {
  _left_child = left;
  left->_parent = shared_from_this();
}

const std::shared_ptr<ExpressionNode> ExpressionNode::right_child() const { return _right_child; }

void ExpressionNode::set_right_child(const std::shared_ptr<ExpressionNode> &right) {
  _right_child = right;
  right->_parent = shared_from_this();
}

const ExpressionType ExpressionNode::type() const { return _type; }

void ExpressionNode::print(const uint32_t level, std::ostream &out) const {
  out << std::setw(level) << " ";
  out << description() << std::endl;

  if (_left_child) {
    _left_child->print(level + 2u);
  }

  if (_right_child) {
    _right_child->print(level + 2u);
  }
}

bool ExpressionNode::is_arithmetic_operator() const {
  switch (_type) {
    case ExpressionType::Subtraction:
    case ExpressionType::Addition:
    case ExpressionType::Multiplication:
    case ExpressionType::Division:
    case ExpressionType::Modulo:
    case ExpressionType::Power:
      return true;
    default:
      return false;
  }
}

bool ExpressionNode::is_binary_operator() const {
  if (is_arithmetic_operator()) return true;

  switch (_type) {
    case ExpressionType::Equals:
    case ExpressionType::NotEquals:
    case ExpressionType::LessThan:
    case ExpressionType::LessThanEquals:
    case ExpressionType::GreaterThan:
    case ExpressionType::GreaterThanEquals:
    case ExpressionType::Like:
    case ExpressionType::NotLike:
    case ExpressionType::And:
    case ExpressionType::Or:
    case ExpressionType::Between:
      return true;
    default:
      return false;
  }
}

const std::string ExpressionNode::description() const {
  std::ostringstream desc;

  desc << "Expression (" << expression_type_to_string.at(_type) << ")";

  switch (_type) {
    case ExpressionType::Literal:
      desc << "[" << value() << "]";
      break;
    case ExpressionType::ColumnReference:
      desc << "[Table: " << table_name() << ", Column: " << name()
           << ", Alias: " << (_alias ? *_alias : std::string("/")) << "]";
      break;
    case ExpressionType::FunctionReference:
      desc << "[" << name() << ": " << std::endl;
      for (const auto &expr : expression_list()) {
        desc << expr->description() << ", " << std::endl;
      }
      desc << "]";
      break;
    case ExpressionType::Select:
      desc << "[" << (_alias ? *_alias : std::string("/")) << "]";
      break;
    default: {}
  }

  return desc.str();
}

const std::string &ExpressionNode::table_name() const {
  DebugAssert(_type == ExpressionType::ColumnReference,
              "Expression other than ColumnReference does not have table_name");
  return _table_name;
}

const std::string &ExpressionNode::name() const {
  DebugAssert(_type == ExpressionType::ColumnReference || _type == ExpressionType::FunctionReference,
              "Expression " + expression_type_to_string.at(_type) + " does not have a name");
  return _name;
}

const optional<std::string> &ExpressionNode::alias() const { return _alias; }

const AllTypeVariant ExpressionNode::value() const {
  DebugAssert(_type == ExpressionType::Literal,
              "Expression " + expression_type_to_string.at(_type) + " does not have a value");
  return _value;
}

std::string ExpressionNode::to_expression_string() const {
  if (_type == ExpressionType::Literal) {
    return type_cast<std::string>(_value);
  } else if (_type == ExpressionType::ColumnReference) {
    return _name;
  } else if (is_arithmetic_operator()) {
    // TODO(mp) Should be is_operator() to also support ExpressionType::Equals, ...
    Assert(static_cast<bool>(left_child()) && static_cast<bool>(right_child()), "Operator needs both operands");

    return left_child()->to_expression_string() + expression_type_to_operator_string.at(_type) +
           right_child()->to_expression_string();
  } else if (_type == ExpressionType::FunctionReference) {
    return _name + "()";
  } else {
    Fail("To generate expression string, ExpressionNodes need to be operators or operands");
  }

  // Should never be reached, but Clang is complaining about missing return statement
  return "";
}

const std::vector<std::shared_ptr<ExpressionNode>> &ExpressionNode::expression_list() const { return _expression_list; }

}  // namespace opossum
