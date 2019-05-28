#pragma once

#include <boost/bimap.hpp>
#include <string>
#include <unordered_map>

namespace opossum {

enum class JitExpressionType;
enum class PredicateCondition;

extern const boost::bimap<JitExpressionType, std::string> jit_expression_type_to_string;
extern const boost::bimap<JitExpressionType, PredicateCondition> jit_expression_type_to_predicate_condition;

}  // namespace opossum
