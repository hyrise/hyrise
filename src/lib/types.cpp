#include "types.hpp"

namespace opossum {

bool is_unary_predicate_condition(const PredicateCondition predicate_condition) {
  return predicate_condition == PredicateCondition::IsNull || predicate_condition == PredicateCondition::IsNotNull;
}

bool is_binary_predicate_condition(const PredicateCondition predicate_condition) {
  return predicate_condition == PredicateCondition::Equals ||
  predicate_condition == PredicateCondition::NotEquals ||
  predicate_condition == PredicateCondition::LessThan ||
  predicate_condition == PredicateCondition::LessThanEquals ||
  predicate_condition == PredicateCondition::GreaterThan ||
  predicate_condition == PredicateCondition::GreaterThanEquals ||
  predicate_condition == PredicateCondition::In ||
  predicate_condition == PredicateCondition::Like;
}

bool is_ternary_predicate_condition(const PredicateCondition predicate_condition) {
  return predicate_condition == PredicateCondition::Between;
}

}  // namespace opossum
