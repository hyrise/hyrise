#include "constant_mappings.hpp"

#include <string>
#include <unordered_map>

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

const std::unordered_map<const ExpressionType, std::string> expression_type_to_string = {
    {ExpressionType::Literal, "Literal"},
    {ExpressionType::Star, "Star"},
    {ExpressionType::Parameter, "Parameter"},
    {ExpressionType::ColumnReference, "ColumnReference"},
    {ExpressionType::FunctionReference, "FunctionReference"},
    {ExpressionType::Operator, "Operator"},
    {ExpressionType::Select, "Select"},
    {ExpressionType::Plus, "Plus"},
    {ExpressionType::Minus, "Minus"},
    {ExpressionType::Asterisk, "Asterisk"},
    {ExpressionType::Slash, "Slash"},
    {ExpressionType::Percentage, "Percentage"},
    {ExpressionType::Caret, "Caret"},
    {ExpressionType::Equals, "Equals"},
    {ExpressionType::NotEquals, "NotEquals"},
    {ExpressionType::Less, "Less"},
    {ExpressionType::LessEquals, "LessEquals"},
    {ExpressionType::Greater, "Greater"},
    {ExpressionType::GreaterEquals, "GreaterEquals"},
    {ExpressionType::Like, "Like"},
    {ExpressionType::NotLike, "NotLike"},
    {ExpressionType::And, "And"},
    {ExpressionType::Or, "Or"},
    {ExpressionType::In, "In"},
    {ExpressionType::Not, "Not"},
    {ExpressionType::IsNull, "IsNull"},
    {ExpressionType::Exists, "Exists"},
    {ExpressionType::Between, "Between"},
    {ExpressionType::Hint, "Hint"},
    {ExpressionType::Case, "Case"},
};

}  // namespace opossum
