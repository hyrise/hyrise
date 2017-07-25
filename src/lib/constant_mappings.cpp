#include "constant_mappings.hpp"

#include <string>
#include <unordered_map>

#include "sql/Expr.h"

namespace opossum {

const std::unordered_map<std::string, proto::ScanType> string_to_proto_scan_type = {
    {"=", opossum::proto::ScanType::OpEquals},        {"!=", opossum::proto::ScanType::OpNotEquals},
    {"<", opossum::proto::ScanType::OpLessThan},      {"<=", opossum::proto::ScanType::OpLessThanEquals},
    {">", opossum::proto::ScanType::OpGreaterThan},   {">=", opossum::proto::ScanType::OpGreaterThanEquals},
    {"BETWEEN", opossum::proto::ScanType::OpBetween}, {"LIKE", opossum::proto::ScanType::OpLike},
};

const std::unordered_map<std::string, ScanType> string_to_scan_type = {
    {"=", opossum::ScanType::OpEquals},        {"!=", opossum::ScanType::OpNotEquals},
    {"<", opossum::ScanType::OpLessThan},      {"<=", opossum::ScanType::OpLessThanEquals},
    {">", opossum::ScanType::OpGreaterThan},   {">=", opossum::ScanType::OpGreaterThanEquals},
    {"BETWEEN", opossum::ScanType::OpBetween}, {"LIKE", opossum::ScanType::OpLike},
};

const std::unordered_map<ScanType, std::string> scan_type_to_string = {
    {opossum::ScanType::OpEquals, "="},        {opossum::ScanType::OpNotEquals, "!="},
    {opossum::ScanType::OpLessThan, "<"},      {opossum::ScanType::OpLessThanEquals, "<="},
    {opossum::ScanType::OpGreaterThan, ">"},   {opossum::ScanType::OpGreaterThanEquals, ">="},
    {opossum::ScanType::OpBetween, "BETWEEN"}, {opossum::ScanType::OpLike, "LIKE"},
};

const std::unordered_map<ExpressionType, std::string> expression_type_to_string = {
    {ExpressionType::Literal, "Literal"},
    {ExpressionType::Star, "Star"},
    {ExpressionType::Parameter, "Parameter"},
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
