#include "expression_node.hpp"

#include <iomanip>
#include <iostream>
#include <memory>
#include <sstream>
#include <string>
#include <unordered_map>
#include <vector>

#include "optimizer/abstract_syntax_tree/abstract_ast_node.hpp"

#include "all_type_variant.hpp"
#include "common.hpp"
#include "constant_mappings.hpp"
#include "type_cast.hpp"
#include "utils/assert.hpp"

namespace opossum {

ExpressionNode::ExpressionNode(const ExpressionType type, const AllTypeVariant &value,
                               const std::vector<std::shared_ptr<ExpressionNode>> &expression_list,
                               const std::string &name, const ColumnID column_id, const optional<std::string> &alias)
    : _type(type),
      _value(value),
      _expression_list(expression_list),
      _name(name),
      _column_id(column_id),
      _alias(alias) {}

std::shared_ptr<ExpressionNode> ExpressionNode::create_expression(const ExpressionType type) {
  const std::vector<std::shared_ptr<ExpressionNode>> expr_list;
  return std::make_shared<ExpressionNode>(type, NULL_VALUE, expr_list, "", ColumnID{});
}

std::shared_ptr<ExpressionNode> ExpressionNode::create_column_reference(const ColumnID column_id,
                                                                        const optional<std::string> &alias) {
  const std::vector<std::shared_ptr<ExpressionNode>> expr_list;
  return std::make_shared<ExpressionNode>(ExpressionType::ColumnReference, NULL_VALUE, expr_list, "", column_id, alias);
}

std::vector<std::shared_ptr<ExpressionNode>> ExpressionNode::create_column_references(
    const std::vector<ColumnID> &column_ids, const std::vector<std::string> &aliases) {
  std::vector<std::shared_ptr<ExpressionNode>> column_references;
  column_references.reserve(column_ids.size());

  if (aliases.empty()) {
    for (auto column_index = 0u; column_index < column_ids.size(); ++column_index) {
      column_references.emplace_back(create_column_reference(column_ids[column_index]));
    }
  } else {
    DebugAssert(column_ids.size() == aliases.size(),
                "There must be the same number of aliases as ColumnIDs, or none at all.");

    for (auto column_index = 0u; column_index < column_ids.size(); ++column_index) {
      column_references.emplace_back(create_column_reference(column_ids[column_index], aliases[column_index]));
    }
  }

  return column_references;
}

std::shared_ptr<ExpressionNode> ExpressionNode::create_literal(const AllTypeVariant &value,
                                                               const optional<std::string> &alias) {
  const std::vector<std::shared_ptr<ExpressionNode>> expr_list;
  return std::make_shared<ExpressionNode>(ExpressionType::Literal, value, expr_list, "", ColumnID{});
}

std::shared_ptr<ExpressionNode> ExpressionNode::create_parameter(const AllTypeVariant &value) {
  const std::vector<std::shared_ptr<ExpressionNode>> expr_list;
  return std::make_shared<ExpressionNode>(ExpressionType::Placeholder, value, expr_list, "", ColumnID{});
}

std::shared_ptr<ExpressionNode> ExpressionNode::create_function_reference(
    const std::string &function_name, const std::vector<std::shared_ptr<ExpressionNode>> &expression_list,
    const optional<std::string> &alias) {
  return std::make_shared<ExpressionNode>(ExpressionType::FunctionReference, NULL_VALUE, expression_list, function_name,
                                          ColumnID{}, alias);
}

std::shared_ptr<ExpressionNode> ExpressionNode::create_binary_operator(ExpressionType type,
                                                                       const std::shared_ptr<ExpressionNode> &left,
                                                                       const std::shared_ptr<ExpressionNode> &right,
                                                                       const optional<std::string> &alias) {
  auto expression = std::make_shared<ExpressionNode>(
      type, AllTypeVariant(), std::vector<std::shared_ptr<ExpressionNode>>(), "", ColumnID{}, alias);
  Assert(expression->is_binary_operator(), "Type is not an operator type");

  expression->set_left_child(left);
  expression->set_right_child(right);

  return expression;
}

std::shared_ptr<ExpressionNode> ExpressionNode::create_select_all() {
  return std::make_shared<ExpressionNode>(ExpressionType::Star, AllTypeVariant(),
                                          std::vector<std::shared_ptr<ExpressionNode>>(), "", ColumnID{});
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

bool ExpressionNode::is_operand() const {
  return _type == ExpressionType::Literal || _type == ExpressionType::ColumnReference;
}

const std::string ExpressionNode::description() const {
  std::ostringstream desc;

  auto alias_string = _alias ? *_alias : std::string("-");

  desc << "Expression (" << expression_type_to_string.at(_type) << ")";

  switch (_type) {
    case ExpressionType::Literal:
      desc << "[" << value() << "]";
      break;
    case ExpressionType::ColumnReference:
      desc << "[ColumnID: " << column_id() << "]";
      break;
    case ExpressionType::FunctionReference:
      desc << "[" << name() << ": " << std::endl;
      for (const auto &expr : expression_list()) {
        desc << expr->description() << ", " << std::endl;
      }
      desc << "]";
      break;
    case ExpressionType::Select:
      desc << "[" << alias_string << "]";
      break;
    default: {}
  }

  return desc.str();
}

const ColumnID ExpressionNode::column_id() const {
  DebugAssert(_type == ExpressionType::ColumnReference,
              "Expression " + expression_type_to_string.at(_type) + " does not have a column_id");
  return _column_id;
}

void ExpressionNode::set_column_id(const ColumnID column_id) {
  DebugAssert(_type == ExpressionType::ColumnReference,
              "Expression " + expression_type_to_string.at(_type) + " does not have a column_id");
  _column_id = column_id;
}

const std::string &ExpressionNode::name() const {
  DebugAssert(_type == ExpressionType::FunctionReference,
              "Expression " + expression_type_to_string.at(_type) + " does not have a name");
  return _name;
}

const optional<std::string> &ExpressionNode::alias() const { return _alias; }

void ExpressionNode::set_alias(const std::string &alias) { _alias = alias; }

const AllTypeVariant ExpressionNode::value() const {
  DebugAssert(_type == ExpressionType::Literal,
              "Expression " + expression_type_to_string.at(_type) + " does not have a value");
  return _value;
}

std::string ExpressionNode::to_string(const std::shared_ptr<AbstractASTNode> &input_node) const {
  std::string column_name;
  switch (_type) {
    case ExpressionType::Literal:
      return type_cast<std::string>(_value);
    case ExpressionType::ColumnReference:
      if (input_node != nullptr) {
        // return input_node->get_column_name_for_column_id(_column_id);
        column_name = input_node->get_column_name_for_column_id(_column_id);
        return column_name;
      }
      return boost::lexical_cast<std::string>(_column_id);
    case ExpressionType::FunctionReference:
      // TODO(tim): BLOCKING - explain why exactly this works (probably because of input node)
      return _expression_list[0]->to_string(input_node);
    default:
      // Handled further down.
      break;
  }

  // TODO(mp): Should be is_operator() to also support ExpressionType::Equals, ...
  Assert(is_arithmetic_operator(), "To generate expression string, ExpressionNodes need to be operators or operands.");
  Assert(static_cast<bool>(left_child()) && static_cast<bool>(right_child()), "Operator needs both operands.");

  return left_child()->to_string(input_node) + expression_type_to_operator_string.at(_type) +
         right_child()->to_string(input_node);
}

const std::vector<std::shared_ptr<ExpressionNode>> &ExpressionNode::expression_list() const { return _expression_list; }

void ExpressionNode::set_expression_list(const std::vector<std::shared_ptr<ExpressionNode>> &expression_list) {
  _expression_list = expression_list;
}

}  // namespace opossum
