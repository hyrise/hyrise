#include "types.hpp"

namespace opossum {

bool is_binary_predicate_condition(const PredicateCondition predicate_condition) {
  return predicate_condition == PredicateCondition::Equals || predicate_condition == PredicateCondition::NotEquals ||
         predicate_condition == PredicateCondition::LessThan ||
         predicate_condition == PredicateCondition::LessThanEquals ||
         predicate_condition == PredicateCondition::GreaterThan ||
         predicate_condition == PredicateCondition::GreaterThanEquals ||
         predicate_condition == PredicateCondition::NotLike || predicate_condition == PredicateCondition::Like;
}

PredicateCondition flip_predicate_condition(const PredicateCondition predicate_condition) {
  switch (predicate_condition) {
    case PredicateCondition::Equals:
      return PredicateCondition::Equals;
    case PredicateCondition::NotEquals:
      return PredicateCondition::NotEquals;
    case PredicateCondition::LessThan:
      return PredicateCondition::GreaterThan;
    case PredicateCondition::LessThanEquals:
      return PredicateCondition::GreaterThanEquals;
    case PredicateCondition::GreaterThan:
      return PredicateCondition::LessThan;
    case PredicateCondition::GreaterThanEquals:
      return PredicateCondition::LessThanEquals;

    case PredicateCondition::Between:
    case PredicateCondition::In:
    case PredicateCondition::Like:
    case PredicateCondition::NotLike:
    case PredicateCondition::IsNull:
    case PredicateCondition::IsNotNull:
      Fail("Can't flip specified PredicateCondition");
  }
  Fail("GCC thinks this is reachable");
}

PredicateCondition inverse_predicate_condition(const PredicateCondition predicate_condition) {
  switch (predicate_condition) {
    case PredicateCondition::Equals:
      return PredicateCondition::NotEquals;
    case PredicateCondition::NotEquals:
      return PredicateCondition::Equals;
    case PredicateCondition::GreaterThan:
      return PredicateCondition::LessThanEquals;
    case PredicateCondition::LessThanEquals:
      return PredicateCondition::GreaterThan;
    case PredicateCondition::GreaterThanEquals:
      return PredicateCondition::LessThan;
    case PredicateCondition::LessThan:
      return PredicateCondition::GreaterThanEquals;
    case PredicateCondition::Like:
      return PredicateCondition::NotLike;
    case PredicateCondition::NotLike:
      return PredicateCondition::Like;
    case PredicateCondition::IsNull:
      return PredicateCondition::IsNotNull;
    case PredicateCondition::IsNotNull:
      return PredicateCondition::IsNull;

    default:
      Fail("Can't inverse the specified PredicateCondition");
  }
}

}  // namespace opossum
