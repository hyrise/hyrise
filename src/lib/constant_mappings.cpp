#include "constant_mappings.hpp"

#include <boost/bimap.hpp>
#include <string>
#include <unordered_map>

#include "sql/Expr.h"

namespace opossum {

/*
 * boost::bimap does not support initializer_lists.
 * Instead we use this helper function to have an initializer_list-friendly interface.
 */
template <typename L, typename R>
boost::bimap<L, R> make_bimap(std::initializer_list<typename boost::bimap<L, R>::value_type> list) {
  return boost::bimap<L, R>(list.begin(), list.end());
}

const std::unordered_map<std::string, proto::ScanType> string_to_proto_scan_type = {
    {"=", opossum::proto::ScanType::OpEquals},        {"!=", opossum::proto::ScanType::OpNotEquals},
    {"<", opossum::proto::ScanType::OpLessThan},      {"<=", opossum::proto::ScanType::OpLessThanEquals},
    {">", opossum::proto::ScanType::OpGreaterThan},   {">=", opossum::proto::ScanType::OpGreaterThanEquals},
    {"BETWEEN", opossum::proto::ScanType::OpBetween}, {"LIKE", opossum::proto::ScanType::OpLike},
};

const boost::bimap<ScanType, std::string> scan_type_to_string = make_bimap<ScanType, std::string>({
    {ScanType::OpEquals, "="},
    {ScanType::OpNotEquals, "!="},
    {ScanType::OpLessThan, "<"},
    {ScanType::OpLessThanEquals, "<="},
    {ScanType::OpGreaterThan, ">"},
    {ScanType::OpGreaterThanEquals, ">="},
    {ScanType::OpBetween, "BETWEEN"},
    {ScanType::OpLike, "LIKE"},
});

const std::unordered_map<ExpressionType, std::string> expression_type_to_string = {
    {ExpressionType::Literal, "Literal"},
    {ExpressionType::Star, "Star"},
    {ExpressionType::Placeholder, "Parameter"},
    {ExpressionType::ColumnReference, "ColumnReference"},
    {ExpressionType::FunctionReference, "FunctionReference"},
    {ExpressionType::Select, "Select"},
    /*Arithmetic operators*/
    {ExpressionType::Plus, "Plus"},
    {ExpressionType::Minus, "Minus"},
    {ExpressionType::Asterisk, "Asterisk"},
    {ExpressionType::Slash, "Slash"},
    {ExpressionType::Percentage, "Percentage"},
    {ExpressionType::Caret, "Caret"},
    /*Logical operators*/
    {ExpressionType::Equals, "Equals"},
    {ExpressionType::NotEquals, "NotEquals"},
    {ExpressionType::LessThan, "LessThan"},
    {ExpressionType::LessThanEquals, "LessThanEquals"},
    {ExpressionType::GreaterThan, "GreaterThan"},
    {ExpressionType::GreaterThanEquals, "GreaterThanEquals"},
    {ExpressionType::Like, "Like"},
    {ExpressionType::NotLike, "NotLike"},
    {ExpressionType::And, "And"},
    {ExpressionType::Or, "Or"},
    {ExpressionType::Between, "Between"},
    {ExpressionType::Not, "Not"},
    /*Set operators*/
    {ExpressionType::In, "In"},
    {ExpressionType::Exists, "Exists"},
    /*Other*/
    {ExpressionType::IsNull, "IsNull"},
    {ExpressionType::Case, "Case"},
    {ExpressionType::Hint, "Hint"},
};

const std::unordered_map<hsql::OperatorType, ExpressionType> operator_type_to_expression_type = {
    {hsql::kOpPlus, ExpressionType::Plus},
    {hsql::kOpMinus, ExpressionType::Minus},
    {hsql::kOpAsterisk, ExpressionType::Asterisk},
    {hsql::kOpSlash, ExpressionType::Slash},
    {hsql::kOpPercentage, ExpressionType::Percentage},
    {hsql::kOpCaret, ExpressionType::Caret},
    {hsql::kOpBetween, ExpressionType::Between},
    {hsql::kOpEquals, ExpressionType::Equals},
    {hsql::kOpNotEquals, ExpressionType::NotEquals},
    {hsql::kOpLess, ExpressionType::LessThan},
    {hsql::kOpLessEq, ExpressionType::LessThanEquals},
    {hsql::kOpGreater, ExpressionType::GreaterThan},
    {hsql::kOpGreaterEq, ExpressionType::GreaterThanEquals},
    {hsql::kOpLike, ExpressionType::Like},
    {hsql::kOpNotLike, ExpressionType::NotLike},
    {hsql::kOpCase, ExpressionType::Case},
    {hsql::kOpExists, ExpressionType::Exists},
    {hsql::kOpIn, ExpressionType::In},
    {hsql::kOpIsNull, ExpressionType::IsNull},
    {hsql::kOpOr, ExpressionType::Or},
};

const std::unordered_map<ExpressionType, std::string> expression_type_to_operator_string = {
    {ExpressionType::Plus, "+"},  {ExpressionType::Minus, "-"},      {ExpressionType::Asterisk, "*"},
    {ExpressionType::Slash, "/"}, {ExpressionType::Percentage, "%"}, {ExpressionType::Caret, "^"},
};

}  // namespace opossum
