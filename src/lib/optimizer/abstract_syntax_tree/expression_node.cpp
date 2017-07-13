#include "expression_node.hpp"

#include <sstream>
#include <string>
#include <type_cast.hpp>

#include "all_type_variant.hpp"
#include "constant_mappings.hpp"
#include "common.hpp"
#include "utils/assert.hpp"

namespace opossum {

const std::unordered_map<ExpressionType, std::string> expression_type_to_string = {
  {ExpressionType::ExpressionPlus, "+"},     {ExpressionType::ExpressionMinus, "-"},
  {ExpressionType::ExpressionAsterisk, "*"}, {ExpressionType::ExpressionSlash, "/"},
};

ExpressionNode::ExpressionNode(const ExpressionType type)
    : AbstractAstNode(AstNodeType::Expression),
      _type(type),
      _value(NULL_VALUE) /*, _value2(NULL_VALUE)*/,
      _name("-"),
      _table("-") {}

ExpressionNode::ExpressionNode(const ExpressionType type, const std::string &table_name, const std::string &column_name)
    : AbstractAstNode(AstNodeType::Expression),
      _type(type),
      _value(NULL_VALUE) /*, _value2(NULL_VALUE)*/,
      _name(column_name),
      _table(table_name) {}

ExpressionNode::ExpressionNode(const ExpressionType type, const AllTypeVariant value /*, const AllTypeVariant value2*/)
    : AbstractAstNode(AstNodeType::Expression), _type(type), _value(value) /*, _value2(value2)*/, _name("-"), _table("-") {}

AggregateFunction ExpressionNode::as_aggregate_function() const {
  Fail("Can't do this right now");
}

std::string ExpressionNode::description() const {
  std::ostringstream desc;

  desc << "Expression (" << _type_to_string() << ", " << value() /*<< ", " << value2()*/ << ", " << table_name() << ", "
       << column_name() << ")";

  return desc.str();
}

bool ExpressionNode::is_arithmetic() const {
  return _type == ExpressionType::ExpressionMinus || _type == ExpressionType::ExpressionPlus
         || _type == ExpressionType::ExpressionAsterisk || _type == ExpressionType::ExpressionSlash;
}

bool ExpressionNode::is_operand() const {
  return
    _type == ExpressionType::ExpressionLiteral ||
    _type == ExpressionType::ExpressionColumnReference;
}

const std::string &ExpressionNode::table_name() const { return _table; }

const std::string &ExpressionNode::column_name() const { return _name; }

const AllTypeVariant ExpressionNode::value() const { return _value; }

// const AllTypeVariant ExpressionNode::value2() const {
//  return _value2;
//}

const ExpressionType ExpressionNode::expression_type() const { return _type; }

std::string ExpressionNode::to_expression_string() const {
  if (is_operand()) {
    return type_cast<std::string>(_value);
  } else if (is_arithmetic()) { // TODO(mp) Should be is_operator() to also support "=", ...
    Assert(static_cast<bool>(left()) && static_cast<bool>(right()), "Operator needs both operands");
    auto left_expression_node = std::static_pointer_cast<ExpressionNode>(left());
    auto right_expression_node = std::static_pointer_cast<ExpressionNode>(right());
    Assert(static_cast<bool>(left_expression_node) &&
             static_cast<bool>(right_expression_node), "Operator needs both operands to be expressions");

    return left_expression_node->to_expression_string() + expression_type_to_string.at(_type) +
      right_expression_node->to_expression_string();
  } else {
    Fail("To generate expression string, ExpressionNodes need to be operators or operands");
  }
}

const std::string ExpressionNode::_type_to_string() const {
  switch (_type) {
    case ExpressionType::ExpressionLiteral:
      return "ExpressionLiteral";
    case ExpressionType::ExpressionStar:
      return "ExpressionStar";
    case ExpressionType::ExpressionParameter:
      return "ExpressionParameter";
    case ExpressionType::ExpressionColumnReference:
      return "ExpressionColumnReference";
    case ExpressionType::ExpressionFunctionReference:
      return "ExpressionFunctionReference";
    case ExpressionType::ExpressionOperator:
      return "ExpressionOperator";
    case ExpressionType::ExpressionSelect:
      return "ExpressionSelect";
    case ExpressionType::ExpressionPlus:
      return "ExpressionPlus";
    case ExpressionType::ExpressionMinus:
      return "ExpressionMinus";
    case ExpressionType::ExpressionAsterisk:
      return "ExpressionAsterisk";
    case ExpressionType::ExpressionSlash:
      return "ExpressionSlash";
    case ExpressionType::ExpressionPercentage:
      return "ExpressionPercentage";
    case ExpressionType::ExpressionCaret:
      return "ExpressionCaret";
    case ExpressionType::ExpressionEquals:
      return "ExpressionEquals";
    case ExpressionType::ExpressionNotEquals:
      return "ExpressionNotEquals";
    case ExpressionType::ExpressionLess:
      return "ExpressionLess";
    case ExpressionType::ExpressionLessEq:
      return "ExpressionLessEq";
    case ExpressionType::ExpressionGreater:
      return "ExpressionGreater";
    case ExpressionType::ExpressionGreaterEq:
      return "ExpressionGreaterEq";
    case ExpressionType::ExpressionLike:
      return "ExpressionLike";
    case ExpressionType::ExpressionNotLike:
      return "ExpressionNotLike";
    case ExpressionType::ExpressionAnd:
      return "ExpressionAnd";
    case ExpressionType::ExpressionOr:
      return "ExpressionOr";
    case ExpressionType::ExpressionIn:
      return "ExpressionIn";
    case ExpressionType::ExpressionNot:
      return "ExpressionNot";
    case ExpressionType::ExpressionIsNull:
      return "ExpressionIsNull";
    case ExpressionType::ExpressionExists:
      return "ExpressionExists";
    case ExpressionType::ExpressionBetween:
      return "ExpressionBetween";
    case ExpressionType::ExpressionHint:
      return "ExpressionHint";
    case ExpressionType::ExpressionCase:
      return "ExpressionCase";
    default:
      throw std::runtime_error("Unexpected expression type");
  }
}

}  // namespace opossum
