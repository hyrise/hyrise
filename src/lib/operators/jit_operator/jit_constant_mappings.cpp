#include "jit_constant_mappings.hpp"

#include <boost/bimap.hpp>
#include <boost/hana/fold.hpp>

#include <string>
#include <unordered_map>

#include "jit_types.hpp"
#include "types.hpp"
#include "utils/make_bimap.hpp"

namespace opossum {

const boost::bimap<JitExpressionType, std::string> jit_expression_type_to_string =
    make_bimap<JitExpressionType, std::string>({{JitExpressionType::Column, "<COLUMN>"},
                                                {JitExpressionType::Value, "<VALUE>"},
                                                {JitExpressionType::Addition, "+"},
                                                {JitExpressionType::Subtraction, "-"},
                                                {JitExpressionType::Multiplication, "*"},
                                                {JitExpressionType::Division, "/"},
                                                {JitExpressionType::Modulo, "%"},
                                                {JitExpressionType::Power, "^"},
                                                {JitExpressionType::Equals, "="},
                                                {JitExpressionType::NotEquals, "<>"},
                                                {JitExpressionType::GreaterThan, ">"},
                                                {JitExpressionType::GreaterThanEquals, ">="},
                                                {JitExpressionType::LessThan, "<"},
                                                {JitExpressionType::LessThanEquals, "<="},
                                                {JitExpressionType::Like, "LIKE"},
                                                {JitExpressionType::NotLike, "NOT LIKE"},
                                                {JitExpressionType::And, "AND"},
                                                {JitExpressionType::Or, "OR"},
                                                {JitExpressionType::Not, "NOT"},
                                                {JitExpressionType::IsNull, "IS NULL"},
                                                {JitExpressionType::IsNotNull, "IS NOT NULL"}});

const boost::bimap<JitExpressionType, PredicateCondition> jit_expression_type_to_predicate_condition =
    make_bimap<JitExpressionType, PredicateCondition>(
        {{JitExpressionType::Equals, PredicateCondition::Equals},
         {JitExpressionType::NotEquals, PredicateCondition::NotEquals},
         {JitExpressionType::GreaterThan, PredicateCondition::GreaterThan},
         {JitExpressionType::GreaterThanEquals, PredicateCondition::GreaterThanEquals},
         {JitExpressionType::LessThan, PredicateCondition::LessThan},
         {JitExpressionType::LessThanEquals, PredicateCondition::LessThanEquals},
         {JitExpressionType::Like, PredicateCondition::Like},
         {JitExpressionType::NotLike, PredicateCondition::NotLike},
         {JitExpressionType::IsNull, PredicateCondition::IsNull},
         {JitExpressionType::IsNotNull, PredicateCondition::IsNotNull}});

}  // namespace opossum
