#include "abstract_expression.hpp"

#include <iomanip>
#include <memory>
#include <optional>
#include <sstream>
#include <string>
#include <vector>

#include "all_type_variant.hpp"
#include "constant_mappings.hpp"
#include "logical_query_plan/abstract_lqp_node.hpp"
#include "logical_query_plan/lqp_expression.hpp"
#include "operators/pqp_expression.hpp"
#include "type_cast.hpp"
#include "utils/assert.hpp"

namespace opossum {

template <typename DerivedExpression>
AbstractExpression<DerivedExpression>::AbstractExpression(ExpressionType type) : _type(type) {}

template <typename DerivedExpression>
std::shared_ptr<DerivedExpression> AbstractExpression<DerivedExpression>::deep_copy() const {
  // We cannot use the copy constructor here, because it does not work with shared_from_this()
  auto deep_copy = std::make_shared<DerivedExpression>(_type);
  deep_copy->_value = _value;
  deep_copy->_aggregate_function = _aggregate_function;
  deep_copy->_table_name = _table_name;
  deep_copy->_alias = _alias;
  deep_copy->_value_placeholder = _value_placeholder;

  if (!_aggregate_function_arguments.empty()) {
    std::vector<std::shared_ptr<DerivedExpression>> aggregate_function_arguments;
    aggregate_function_arguments.reserve(_aggregate_function_arguments.size());
    for (const auto& expression : _aggregate_function_arguments) {
      aggregate_function_arguments.emplace_back(expression->deep_copy());
    }
    deep_copy->_aggregate_function_arguments = std::move(aggregate_function_arguments);
  }

  if (left_child()) deep_copy->set_left_child(left_child()->deep_copy());
  if (right_child()) deep_copy->set_right_child(right_child()->deep_copy());

  _deep_copy_impl(deep_copy);

  return deep_copy;
}

template <typename DerivedExpression>
const std::shared_ptr<DerivedExpression> AbstractExpression<DerivedExpression>::left_child() const {
  return _left_child;
}

template <typename DerivedExpression>
void AbstractExpression<DerivedExpression>::set_left_child(const std::shared_ptr<DerivedExpression>& left) {
  _left_child = left;
}

template <typename DerivedExpression>
const std::shared_ptr<DerivedExpression> AbstractExpression<DerivedExpression>::right_child() const {
  return _right_child;
}

template <typename DerivedExpression>
void AbstractExpression<DerivedExpression>::set_right_child(const std::shared_ptr<DerivedExpression>& right) {
  _right_child = right;
}

template <typename DerivedExpression>
ExpressionType AbstractExpression<DerivedExpression>::type() const {
  return _type;
}

template <typename DerivedExpression>
void AbstractExpression<DerivedExpression>::print(const uint32_t level, std::ostream& out) const {
  out << std::setw(level) << " ";
  out << description() << std::endl;

  if (_left_child) {
    _left_child->print(level + 2u);
  }

  if (_right_child) {
    _right_child->print(level + 2u);
  }
}

template <typename DerivedExpression>
bool AbstractExpression<DerivedExpression>::is_operator() const {
  return is_arithmetic_operator() || is_logical_operator();
}

template <typename DerivedExpression>
bool AbstractExpression<DerivedExpression>::is_arithmetic_operator() const {
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

template <typename DerivedExpression>
bool AbstractExpression<DerivedExpression>::is_logical_operator() const {
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

template <typename DerivedExpression>
bool AbstractExpression<DerivedExpression>::is_binary_operator() const {
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

template <typename DerivedExpression>
bool AbstractExpression<DerivedExpression>::is_unary_operator() const {
  switch (_type) {
    case ExpressionType::Not:
    case ExpressionType::Exists:
      return true;
    default:
      return false;
  }
}

template <typename DerivedExpression>
bool AbstractExpression<DerivedExpression>::is_null_literal() const {
  return _type == ExpressionType::Literal && _value && variant_is_null(*_value);
}

template <typename DerivedExpression>
bool AbstractExpression<DerivedExpression>::is_subselect() const {
  return _type == ExpressionType::Subselect;
}

template <typename DerivedExpression>
bool AbstractExpression<DerivedExpression>::is_operand() const {
  return _type == ExpressionType::Literal || _type == ExpressionType::Column;
}

template <typename DerivedExpression>
const std::string AbstractExpression<DerivedExpression>::description() const {
  std::ostringstream desc;

  auto alias_string = _alias ? *_alias : std::string("-");

  desc << "Expression (" << expression_type_to_string.at(_type) << ")";

  switch (_type) {
    case ExpressionType::Literal:
      desc << "[" << value() << "]";
      break;
    case ExpressionType::Column:
      desc << "[" << to_string() << "]";
      break;
    case ExpressionType::Function:
      desc << "[" << aggregate_function_to_string.left.at(aggregate_function()) << ": " << std::endl;
      for (const auto& expr : aggregate_function_arguments()) {
        desc << expr->description() << ", " << std::endl;
      }
      desc << "]";
      break;
    case ExpressionType::Subselect:
      desc << "[" << alias_string << "]";
      break;
    default: {}
  }

  return desc.str();
}

template <typename DerivedExpression>
const std::optional<std::string>& AbstractExpression<DerivedExpression>::table_name() const {
  return _table_name;
}

template <typename DerivedExpression>
AggregateFunction AbstractExpression<DerivedExpression>::aggregate_function() const {
  DebugAssert(_aggregate_function != std::nullopt,
              "Expression " + expression_type_to_string.at(_type) + " does not have an aggregate function");
  return *_aggregate_function;
}

template <typename DerivedExpression>
const std::optional<std::string>& AbstractExpression<DerivedExpression>::alias() const {
  return _alias;
}

template <typename DerivedExpression>
const AllTypeVariant AbstractExpression<DerivedExpression>::value() const {
  DebugAssert(_value != std::nullopt, "Expression " + expression_type_to_string.at(_type) + " does not have a value");
  return *_value;
}

template <typename DerivedExpression>
ValuePlaceholder AbstractExpression<DerivedExpression>::value_placeholder() const {
  DebugAssert(_value_placeholder != std::nullopt,
              "Expression " + expression_type_to_string.at(_type) + " does not have a value placeholder");
  return *_value_placeholder;
}

template <typename DerivedExpression>
std::string AbstractExpression<DerivedExpression>::to_string(
    const std::optional<std::vector<std::string>>& input_column_names, bool is_root) const {
  switch (_type) {
    case ExpressionType::Literal:
      if (is_null_literal()) {
        return std::string("NULL");
      }
      if (value().type() == typeid(std::string)) {
        return "\"" + boost::get<std::string>(value()) + "\"";
      }
      return type_cast<std::string>(value());
    case ExpressionType::Placeholder:
      return "?";
    case ExpressionType::Column:
      Fail("This should be handled in derived AbstractExpression type");
      return "";
    case ExpressionType::Function:
      return aggregate_function_to_string.left.at(aggregate_function()) + "(" +
             _aggregate_function_arguments[0]->to_string(input_column_names, true) + ")";
    case ExpressionType::Star:
      return std::string("*");
    case ExpressionType::Subselect:
      return "subquery";
    default:
      // Handled further down.
      break;
  }

  Assert(is_operator(),
         "To generate expression string, Expressions need to be operators or operands (which are already covered "
         "further up).");

  Assert(left_child(), "Operator needs left input.");

  std::string result;
  const auto left_column_name = left_child()->to_string(input_column_names, false);
  const auto& op = expression_type_to_operator_string.at(_type);

  if (is_binary_operator()) {
    Assert(right_child(), "Binary Operator needs both inputs.");

    const auto right_column_name = right_child()->to_string(input_column_names, false);
    result = left_column_name + " " + op + " " + right_column_name;
  } else {
    Assert(!right_child(), "Unary Operator can only have left input.");

    result = op + " " + left_column_name;
  }

  // Don't put brackets around root expression, i.e. generate "5+(a*3)" and not "(5+(a*3))"
  if (!is_root) {
    result = "(" + result + ")";
  }

  return result;
}

template <typename DerivedExpression>
const std::vector<std::shared_ptr<DerivedExpression>>&
AbstractExpression<DerivedExpression>::aggregate_function_arguments() const {
  return _aggregate_function_arguments;
}

template <typename DerivedExpression>
void AbstractExpression<DerivedExpression>::set_aggregate_function_arguments(
    const std::vector<std::shared_ptr<DerivedExpression>>& aggregate_function_arguments) {
  _aggregate_function_arguments = aggregate_function_arguments;
}

template <typename DerivedExpression>
bool AbstractExpression<DerivedExpression>::operator==(const AbstractExpression& other) const {
  auto compare_expression_ptrs = [](const auto& left_pointer, const auto& right_pointer) {
    if (left_pointer && right_pointer) {
      return *left_pointer == *right_pointer;
    }

    return left_pointer == right_pointer;
  };

  if (!compare_expression_ptrs(_left_child, other._left_child)) return false;
  if (!compare_expression_ptrs(_right_child, other._right_child)) return false;

  if (_aggregate_function_arguments.size() != other._aggregate_function_arguments.size()) return false;

  for (size_t expression_list_idx = 0; expression_list_idx < _aggregate_function_arguments.size();
       ++expression_list_idx) {
    if (!compare_expression_ptrs(_aggregate_function_arguments[expression_list_idx],
                                 other._aggregate_function_arguments[expression_list_idx])) {
      return false;
    }
  }

  return _type == other._type && _value == other._value && _aggregate_function == other._aggregate_function &&
         _table_name == other._table_name && _alias == other._alias;
}

template <typename DerivedExpression>
void AbstractExpression<DerivedExpression>::set_alias(const std::string& alias) {
  _alias = alias;
}

template <typename DerivedExpression>
std::shared_ptr<DerivedExpression> AbstractExpression<DerivedExpression>::create_literal(
    const AllTypeVariant& value, const std::optional<std::string>& alias) {
  auto expression = std::make_shared<DerivedExpression>(ExpressionType::Literal);
  expression->_alias = alias;
  expression->_value = value;

  return expression;
}

template <typename DerivedExpression>
std::shared_ptr<DerivedExpression> AbstractExpression<DerivedExpression>::create_value_placeholder(
    ValuePlaceholder value_placeholder) {
  auto expression = std::make_shared<DerivedExpression>(ExpressionType::Placeholder);
  expression->_value_placeholder = value_placeholder;
  return expression;
}

template <typename DerivedExpression>
std::shared_ptr<DerivedExpression> AbstractExpression<DerivedExpression>::create_aggregate_function(
    AggregateFunction aggregate_function, const std::vector<std::shared_ptr<DerivedExpression>>& function_arguments,
    const std::optional<std::string>& alias) {
  auto expression = std::make_shared<DerivedExpression>(ExpressionType::Function);
  expression->_aggregate_function = aggregate_function;
  expression->_aggregate_function_arguments = function_arguments;
  expression->_alias = alias;
  return expression;
}

template <typename DerivedExpression>
std::shared_ptr<DerivedExpression> AbstractExpression<DerivedExpression>::create_binary_operator(
    ExpressionType type, const std::shared_ptr<DerivedExpression>& left,
    const std::shared_ptr<DerivedExpression>& right, const std::optional<std::string>& alias) {
  auto expression = std::make_shared<DerivedExpression>(type);
  Assert(expression->is_binary_operator(),
         "Type is not a binary operator type, such as Equals, LessThan, Like, And, etc.");
  expression->_alias = alias;

  expression->set_left_child(left);
  expression->set_right_child(right);

  return expression;
}

template <typename DerivedExpression>
std::shared_ptr<DerivedExpression> AbstractExpression<DerivedExpression>::create_unary_operator(
    ExpressionType type, const std::shared_ptr<DerivedExpression>& input, const std::optional<std::string>& alias) {
  auto expression = std::make_shared<DerivedExpression>(type);
  Assert(expression->is_unary_operator(), "Type is not a unary operator such as Not, Exists");
  expression->_alias = alias;

  expression->set_left_child(input);

  return expression;
}

template <typename DerivedExpression>
std::shared_ptr<DerivedExpression> AbstractExpression<DerivedExpression>::create_select_star(
    const std::optional<std::string>& table_name) {
  auto expression = std::make_shared<DerivedExpression>(ExpressionType::Star);
  expression->_table_name = table_name;
  return expression;
}

template class AbstractExpression<LQPExpression>;
template class AbstractExpression<PQPExpression>;

}  // namespace opossum
