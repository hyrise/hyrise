#include "expression.hpp"

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

Expression::Expression(ExpressionType type) : _type(type) {}

std::shared_ptr<Expression> Expression::create_column_identifier(const ColumnID column_id,
                                                                 const optional<std::string> &alias) {
  auto expression = std::make_shared<Expression>(ExpressionType::ColumnIdentifier);
  expression->_column_id = column_id;
  expression->_alias = alias;

  return expression;
}

std::vector<std::shared_ptr<Expression>> Expression::create_column_identifiers(
    const std::vector<ColumnID> &column_ids, const optional<std::vector<std::string>> &aliases) {
  std::vector<std::shared_ptr<Expression>> column_references;
  column_references.reserve(column_ids.size());

  if (!aliases) {
    for (const auto column_id : column_ids) {
      column_references.emplace_back(create_column_identifier(column_id));
    }
  } else {
    DebugAssert(column_ids.size() == (*aliases).size(), "There must be the same number of aliases as ColumnIDs");

    for (auto column_index = 0u; column_index < column_ids.size(); ++column_index) {
      column_references.emplace_back(create_column_identifier(column_ids[column_index], (*aliases)[column_index]));
    }
  }

  return column_references;
}

std::shared_ptr<Expression> Expression::create_literal(const AllTypeVariant &value,
                                                       const optional<std::string> &alias) {
  auto expression = std::make_shared<Expression>(ExpressionType::Literal);
  expression->_alias = alias;
  expression->_value = value;

  return expression;
}

std::shared_ptr<Expression> Expression::create_placeholder(const AllTypeVariant &value) {
  auto expression = std::make_shared<Expression>(ExpressionType::Placeholder);
  expression->_value = value;
  return expression;
}

std::shared_ptr<Expression> Expression::create_function(
  const std::string &function_name, const std::vector<std::shared_ptr<Expression>> &expression_list,
  const optional<std::string> &alias) {
  auto expression = std::make_shared<Expression>(ExpressionType::Function);
  expression->_name = function_name;
  expression->_expression_list = expression_list;
  expression->_alias = alias;
  return expression;
}

std::shared_ptr<Expression> Expression::create_binary_operator(ExpressionType type,
                                                               const std::shared_ptr<Expression> &left,
                                                               const std::shared_ptr<Expression> &right,
                                                               const optional<std::string> &alias) {
  auto expression = std::make_shared<Expression>(type);
  Assert(expression->is_binary_operator(),
         "Type is not a binary operator type, such as Equals, LessThan, Like, And, etc.");
  expression->_alias = alias;

  expression->set_left_child(left);
  expression->set_right_child(right);

  return expression;
}

std::shared_ptr<Expression> Expression::create_select_star(const std::string &table_name) {
  auto expression = std::make_shared<Expression>(ExpressionType::Star);
  expression->_name = table_name;
  return expression;
}

const std::weak_ptr<Expression> Expression::parent() const { return _parent; }

void Expression::clear_parent() { _parent.reset(); }

const std::shared_ptr<Expression> Expression::left_child() const { return _left_child; }

void Expression::set_left_child(const std::shared_ptr<Expression> &left) {
  _left_child = left;
  left->_parent = shared_from_this();
}

const std::shared_ptr<Expression> Expression::right_child() const { return _right_child; }

void Expression::set_right_child(const std::shared_ptr<Expression> &right) {
  _right_child = right;
  right->_parent = shared_from_this();
}

const ExpressionType Expression::type() const { return _type; }

void Expression::print(const uint32_t level, std::ostream &out) const {
  out << std::setw(level) << " ";
  out << description() << std::endl;

  if (_left_child) {
    _left_child->print(level + 2u);
  }

  if (_right_child) {
    _right_child->print(level + 2u);
  }
}

bool Expression::is_arithmetic_operator() const {
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

bool Expression::is_binary_operator() const {
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

bool Expression::is_operand() const {
  return _type == ExpressionType::Literal || _type == ExpressionType::ColumnIdentifier;
}

const std::string Expression::description() const {
  std::ostringstream desc;

  auto alias_string = _alias ? *_alias : std::string("-");

  desc << "Expression (" << expression_type_to_string.at(_type) << ")";

  switch (_type) {
    case ExpressionType::Literal:
      desc << "[" << value() << "]";
      break;
    case ExpressionType::ColumnIdentifier:
      desc << "[ColumnID: " << column_id() << "]";
      break;
    case ExpressionType::Function:
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

const ColumnID Expression::column_id() const {
  DebugAssert(_column_id != nullopt,
              "Expression " + expression_type_to_string.at(_type) + " does not have a column_id");
  return *_column_id;
}

const std::string &Expression::name() const {
  DebugAssert(_name != nullopt, "Expression " + expression_type_to_string.at(_type) + " does not have a name");
  return *_name;
}

const optional<std::string> &Expression::alias() const { return _alias; }

const AllTypeVariant Expression::value() const {
  DebugAssert(_value != nullopt, "Expression " + expression_type_to_string.at(_type) + " does not have a value");
  return *_value;
}

std::string Expression::to_string(const std::shared_ptr<AbstractASTNode> &input_node) const {
  std::string column_name;
  switch (_type) {
    case ExpressionType::Literal:
      return type_cast<std::string>(value());
    case ExpressionType::ColumnIdentifier:
      if (input_node != nullptr) {
        DebugAssert(column_id() < input_node->output_column_names().size(), "_column_id out of range");
        return input_node->output_column_names()[column_id()];
      }
      return boost::lexical_cast<std::string>(column_id());
    case ExpressionType::Function:
      return name() + "(" + _expression_list[0]->to_string(input_node) + ")";
    default:
      // Handled further down.
      break;
  }

  // TODO(mp): Should be is_operator() to also support ExpressionType::Equals, ...
  Assert(is_arithmetic_operator(), "To generate expression string, Expression need to be operators or operands.");
  Assert(static_cast<bool>(left_child()) && static_cast<bool>(right_child()), "Operator needs both operands.");

  return left_child()->to_string(input_node) + expression_type_to_operator_string.at(_type) +
         right_child()->to_string(input_node);
}

const std::vector<std::shared_ptr<Expression>> &Expression::expression_list() const { return _expression_list; }

void Expression::set_expression_list(const std::vector<std::shared_ptr<Expression>> &expression_list) {
  _expression_list = expression_list;
}

bool Expression::operator==(const Expression &rhs) const {
  auto compare_expression_ptrs = [](const auto &ptr_lhs, const auto &ptr_rhs) {
    if (ptr_lhs && ptr_rhs) {
      return *ptr_lhs == *ptr_rhs;
    }

    return ptr_lhs == ptr_rhs;
  };

  if (!compare_expression_ptrs(_left_child, rhs._left_child)) return false;
  if (!compare_expression_ptrs(_right_child, rhs._right_child)) return false;

  if (_expression_list.size() != rhs._expression_list.size()) return false;

  for (size_t expression_list_idx = 0; expression_list_idx < _expression_list.size(); ++expression_list_idx) {
    if (!compare_expression_ptrs(_expression_list[expression_list_idx], rhs._expression_list[expression_list_idx])) {
      return false;
    }
  }

  return _type == rhs._type && _value == rhs._value && _name == rhs._name && _column_id == rhs._column_id &&
         _alias == rhs._alias;
}

}  // namespace opossum
