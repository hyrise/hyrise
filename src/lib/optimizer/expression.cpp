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

std::shared_ptr<Expression> Expression::create_column(const ColumnID column_id, const optional<std::string> &alias) {
  auto expression = std::make_shared<Expression>(ExpressionType::Column);
  expression->_column_id = column_id;
  expression->_alias = alias;

  return expression;
}

std::vector<std::shared_ptr<Expression>> Expression::create_columns(const std::vector<ColumnID> &column_ids,
                                                                    const optional<std::vector<std::string>> &aliases) {
  std::vector<std::shared_ptr<Expression>> column_references;
  column_references.reserve(column_ids.size());

  if (!aliases) {
    for (const auto column_id : column_ids) {
      column_references.emplace_back(create_column(column_id));
    }
  } else {
    DebugAssert(column_ids.size() == (*aliases).size(), "There must be the same number of aliases as ColumnIDs");

    for (auto column_index = 0u; column_index < column_ids.size(); ++column_index) {
      column_references.emplace_back(create_column(column_ids[column_index], (*aliases)[column_index]));
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

std::shared_ptr<Expression> Expression::create_value_placeholder(ValuePlaceholder value_placeholder) {
  auto expression = std::make_shared<Expression>(ExpressionType::Placeholder);
  expression->_value_placeholder = value_placeholder;
  return expression;
}

std::shared_ptr<Expression> Expression::create_aggregate_function(
    AggregateFunction aggregate_function, const std::vector<std::shared_ptr<Expression>> &expression_list,
    const optional<std::string> &alias) {
  auto expression = std::make_shared<Expression>(ExpressionType::Function);
  expression->_aggregate_function = aggregate_function;
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

std::shared_ptr<Expression> Expression::create_unary_operator(ExpressionType type,
                                                              const std::shared_ptr<Expression> &input,
                                                              const optional<std::string> &alias) {
  auto expression = std::make_shared<Expression>(type);
  Assert(expression->is_unary_operator(), "Type is not a unary operator such as Not, Exists");
  expression->_alias = alias;

  expression->set_left_child(input);

  return expression;
}

std::shared_ptr<Expression> Expression::create_select_star(const optional<std::string> &table_name) {
  auto expression = std::make_shared<Expression>(ExpressionType::Star);
  expression->_table_name = table_name;
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

bool Expression::is_operator() const { return is_arithmetic_operator() || is_logical_operator(); }

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

bool Expression::is_logical_operator() const {
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
    case ExpressionType::Not:
    case ExpressionType::Exists:
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

bool Expression::is_unary_operator() const {
  switch (_type) {
    case ExpressionType::Not:
    case ExpressionType::Exists:
      return true;
    default:
      return false;
  }
}

bool Expression::is_null_literal() const { return _type == ExpressionType::Literal && _value && is_null(*_value); }

bool Expression::is_operand() const { return _type == ExpressionType::Literal || _type == ExpressionType::Column; }

const std::string Expression::description() const {
  std::ostringstream desc;

  auto alias_string = _alias ? *_alias : std::string("-");

  desc << "Expression (" << expression_type_to_string.at(_type) << ")";

  switch (_type) {
    case ExpressionType::Literal:
      desc << "[" << value() << "]";
      break;
    case ExpressionType::Column:
      desc << "[ColumnID: " << column_id() << "]";
      break;
    case ExpressionType::Function:
      desc << "[" << aggregate_function_to_string.left.at(aggregate_function()) << ": " << std::endl;
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

const optional<std::string> &Expression::table_name() const { return _table_name; }

AggregateFunction Expression::aggregate_function() const {
  DebugAssert(_aggregate_function != nullopt,
              "Expression " + expression_type_to_string.at(_type) + " does not have an aggregate function");
  return *_aggregate_function;
}

const optional<std::string> &Expression::alias() const { return _alias; }

const AllTypeVariant Expression::value() const {
  DebugAssert(_value != nullopt, "Expression " + expression_type_to_string.at(_type) + " does not have a value");
  return *_value;
}

ValuePlaceholder Expression::value_placeholder() const {
  DebugAssert(_value_placeholder != nullopt,
              "Expression " + expression_type_to_string.at(_type) + " does not have a value placeholder");
  return *_value_placeholder;
}

std::string Expression::to_string(const std::vector<std::string> &input_column_names) const {
  switch (_type) {
    case ExpressionType::Literal:
      if (is_null_literal()) {
        return std::string("NULL");
      }
      if (value().type() == typeid(std::string)) {
        return "\"" + boost::get<std::string>(value()) + "\"";
      }
      return type_cast<std::string>(value());
    case ExpressionType::Column:
      if (!input_column_names.empty()) {
        DebugAssert(column_id() < input_column_names.size(),
                    std::string("_column_id ") + std::to_string(column_id()) + " out of range");
        return input_column_names[column_id()];
      }
      return std::string("ColumnID #" + std::to_string(column_id()));
    case ExpressionType::Function:
      return aggregate_function_to_string.left.at(aggregate_function()) + "(" +
             _expression_list[0]->to_string(input_column_names) + ")";
    case ExpressionType::Star:
      return std::string("*");
    default:
      // Handled further down.
      break;
  }

  Assert(is_operator(),
         "To generate expression string, Expressions need to be operators or operands (which are already covered "
         "further up).");

  Assert(left_child(), "Operator needs left child.");

  std::string result;
  const auto lhs = left_child()->to_string(input_column_names);
  const auto &op = expression_type_to_operator_string.at(_type);

  if (is_binary_operator()) {
    Assert(right_child(), "Binary Operator needs both children.");

    const auto rhs = right_child()->to_string(input_column_names);
    result = lhs + " " + op + " " + rhs;
  } else {
    Assert(!right_child(), "Unary Operator can only have left child.");

    result = op + " " + lhs;
  }

  // Don't put brackets around root expression, i.e. generate "5+(a*3)" and not "(5+(a*3))"
  if (_parent.lock()) {
    result = "(" + result + ")";
  }

  return result;
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

  return _type == rhs._type && _value == rhs._value && _table_name == rhs._table_name && _column_id == rhs._column_id &&
         _alias == rhs._alias;
}

void Expression::set_alias(const std::string &alias) { _alias = alias; }

}  // namespace opossum
