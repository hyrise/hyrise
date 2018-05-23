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
  predicate_condition == PredicateCondition::Like;
}
bool is_lexicographical_predicate_condition(const PredicateCondition predicate_condition) {
  return predicate_condition == PredicateCondition::Equals ||
  predicate_condition == PredicateCondition::NotEquals ||
  predicate_condition == PredicateCondition::LessThan ||
  predicate_condition == PredicateCondition::LessThanEquals ||
  predicate_condition == PredicateCondition::GreaterThan ||
  predicate_condition == PredicateCondition::GreaterThanEquals;
}

bool is_ternary_predicate_condition(const PredicateCondition predicate_condition) {
  return predicate_condition == PredicateCondition::Between;
}

PredicateCondition flip_predicate_condition(const PredicateCondition predicate_condition) {
  switch (predicate_condition) {
    case PredicateCondition::Equals: return PredicateCondition::Equals;
    case PredicateCondition::NotEquals: return PredicateCondition::NotEquals;
    case PredicateCondition::LessThan: return PredicateCondition::GreaterThan;
    case PredicateCondition::LessThanEquals: return PredicateCondition::GreaterThanEquals;
    case PredicateCondition::GreaterThan: return PredicateCondition::LessThan;
    case PredicateCondition::GreaterThanEquals: return PredicateCondition::LessThanEquals;

    case PredicateCondition::Between:
    case PredicateCondition::In:
    case PredicateCondition::Like:
    case PredicateCondition::NotLike:
    case PredicateCondition::IsNull:
    case PredicateCondition::IsNotNull:
      Fail("Can't flip specified PredicateCondition");
  }
}

}  // namespace opossum
