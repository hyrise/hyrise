#include "expression_node.hpp"

#include <sstream>
#include <string>

#include "all_type_variant.hpp"
#include "common.hpp"
#include "utils/assert.hpp"

namespace opossum {

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

std::string ExpressionNode::description() const {
  std::ostringstream desc;

  desc << "Expression (" << _type_to_string() << ", " << value() /*<< ", " << value2()*/ << ", " << table_name() << ", "
       << column_name() << ")";

  return desc.str();
}

const std::string &ExpressionNode::table_name() const { return _table; }

const std::string &ExpressionNode::column_name() const { return _name; }

const AllTypeVariant ExpressionNode::value() const { return _value; }

// const AllTypeVariant ExpressionNode::value2() const {
//  return _value2;
//}

const ExpressionType ExpressionNode::expression_type() const { return _type; }

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
