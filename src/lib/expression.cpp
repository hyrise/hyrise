#include "expression.hpp"

#include <iomanip>
#include <memory>
#include <optional>
#include <sstream>
#include <string>
#include <vector>

#include "all_type_variant.hpp"
#include "constant_mappings.hpp"
#include "logical_query_plan/abstract_lqp_node.hpp"
#include "type_cast.hpp"
#include "utils/assert.hpp"

namespace opossum {

Expression::Expression(ExpressionType type) : _type(type) {}

const std::weak_ptr<Expression> Expression::parent() const { return _parent; }

void Expression::clear_parent() { _parent.reset(); }

const std::shared_ptr<Expression> Expression::left_child() const { return _left_child; }

void Expression::set_left_child(const std::shared_ptr<Expression>& left) {
  _left_child = left;
  left->_parent = shared_from_this();
}

const std::shared_ptr<Expression> Expression::right_child() const { return _right_child; }

void Expression::set_right_child(const std::shared_ptr<Expression>& right) {
  _right_child = right;
  right->_parent = shared_from_this();
}

ExpressionType Expression::type() const { return _type; }

void Expression::print(const uint32_t level, std::ostream& out) const {
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

bool Expression::is_null_literal() const {
  return _type == ExpressionType::Literal && _value && variant_is_null(*_value);
}

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
      desc << "[Column: " << column_origin().node->name() << ": " column_origin().column_id << "]";
      break;
    case ExpressionType::Function:
      desc << "[" << aggregate_function_to_string.left.at(aggregate_function()) << ": " << std::endl;
      for (const auto& expr : aggregate_function_arguments()) {
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

const std::optional<std::string>& Expression::table_name() const { return _table_name; }

AggregateFunction Expression::aggregate_function() const {
  DebugAssert(_aggregate_function != std::nullopt,
              "Expression " + expression_type_to_string.at(_type) + " does not have an aggregate function");
  return *_aggregate_function;
}

const std::optional<std::string>& Expression::alias() const { return _alias; }

const AllTypeVariant Expression::value() const {
  DebugAssert(_value != std::nullopt, "Expression " + expression_type_to_string.at(_type) + " does not have a value");
  return *_value;
}

ValuePlaceholder Expression::value_placeholder() const {
  DebugAssert(_value_placeholder != std::nullopt,
              "Expression " + expression_type_to_string.at(_type) + " does not have a value placeholder");
  return *_value_placeholder;
}

std::string Expression::to_string(const std::optional<std::vector<std::string>>& input_column_names) const {
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
      Fail("This should be handled in derived Expression type");
    case ExpressionType::Function:
      return aggregate_function_to_string.left.at(aggregate_function()) + "(" +
             _aggregate_function_arguments[0]->to_string() + ")";
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
  const auto left_column_name = left_child()->to_string();
  const auto& op = expression_type_to_operator_string.at(_type);

  if (is_binary_operator()) {
    Assert(right_child(), "Binary Operator needs both children.");

    const auto right_column_name = right_child()->to_string();
    result = left_column_name + " " + op + " " + right_column_name;
  } else {
    Assert(!right_child(), "Unary Operator can only have left child.");

    result = op + " " + left_column_name;
  }

  // Don't put brackets around root expression, i.e. generate "5+(a*3)" and not "(5+(a*3))"
  if (_parent.lock()) {
    result = "(" + result + ")";
  }

  return result;
}

const std::vector<std::shared_ptr<Expression>>& Expression::aggregate_function_arguments() const { return _aggregate_function_arguments; }

void Expression::set_aggregate_function_arguments(const std::vector<std::shared_ptr<Expression>>& aggregate_function_arguments) {
  _aggregate_function_arguments = aggregate_function_arguments;
}

bool Expression::operator==(const Expression& other) const {
  auto compare_expression_ptrs = [](const auto& left_pointer, const auto& right_pointer) {
    if (left_pointer && right_pointer) {
      return *left_pointer == *right_pointer;
    }

    return left_pointer == right_pointer;
  };

  if (!compare_expression_ptrs(_left_child, other._left_child)) return false;
  if (!compare_expression_ptrs(_right_child, other._right_child)) return false;

  if (_aggregate_function_arguments.size() != other._aggregate_function_arguments.size()) return false;

  for (size_t expression_list_idx = 0; expression_list_idx < _aggregate_function_arguments.size(); ++expression_list_idx) {
    if (!compare_expression_ptrs(_aggregate_function_arguments[expression_list_idx], other._aggregate_function_arguments[expression_list_idx])) {
      return false;
    }
  }

  return _type == other._type && _value == other._value && _aggregate_function == other._aggregate_function &&
         _table_name == other._table_name && _column_id == other._column_id && _alias == other._alias;
}

void Expression::set_alias(const std::string& alias) { _alias = alias; }

}  // namespace opossum
