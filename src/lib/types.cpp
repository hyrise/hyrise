#include "types.hpp"

#include <unordered_map>

#include "magic_enum.hpp"

namespace hyrise {

bool is_binary_predicate_condition(const PredicateCondition predicate_condition) {
  return predicate_condition == PredicateCondition::Equals || predicate_condition == PredicateCondition::NotEquals ||
         predicate_condition == PredicateCondition::LessThan ||
         predicate_condition == PredicateCondition::LessThanEquals ||
         predicate_condition == PredicateCondition::GreaterThan ||
         predicate_condition == PredicateCondition::GreaterThanEquals ||
         predicate_condition == PredicateCondition::NotLike || predicate_condition == PredicateCondition::Like ||
         predicate_condition == PredicateCondition::In || predicate_condition == PredicateCondition::NotIn;
}

bool is_binary_numeric_predicate_condition(const PredicateCondition predicate_condition) {
  return predicate_condition == PredicateCondition::Equals || predicate_condition == PredicateCondition::NotEquals ||
         predicate_condition == PredicateCondition::LessThan ||
         predicate_condition == PredicateCondition::LessThanEquals ||
         predicate_condition == PredicateCondition::GreaterThan ||
         predicate_condition == PredicateCondition::GreaterThanEquals;
}

bool is_between_predicate_condition(PredicateCondition predicate_condition) {
  return predicate_condition == PredicateCondition::BetweenInclusive ||
         predicate_condition == PredicateCondition::BetweenLowerExclusive ||
         predicate_condition == PredicateCondition::BetweenUpperExclusive ||
         predicate_condition == PredicateCondition::BetweenExclusive;
}

bool is_lower_inclusive_between(PredicateCondition predicate_condition) {
  return predicate_condition == PredicateCondition::BetweenInclusive ||
         predicate_condition == PredicateCondition::BetweenUpperExclusive;
}

bool is_upper_inclusive_between(PredicateCondition predicate_condition) {
  return predicate_condition == PredicateCondition::BetweenInclusive ||
         predicate_condition == PredicateCondition::BetweenLowerExclusive;
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

    case PredicateCondition::BetweenInclusive:
    case PredicateCondition::BetweenLowerExclusive:
    case PredicateCondition::BetweenUpperExclusive:
    case PredicateCondition::BetweenExclusive:
    case PredicateCondition::In:
    case PredicateCondition::NotIn:
    case PredicateCondition::Like:
    case PredicateCondition::NotLike:
    case PredicateCondition::IsNull:
    case PredicateCondition::IsNotNull:
      Fail("Can't flip specified PredicateCondition");
  }
  Fail("Invalid enum value");
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
    case PredicateCondition::In:
      return PredicateCondition::NotIn;
    case PredicateCondition::NotIn:
      return PredicateCondition::In;

    default:
      Fail("Can't inverse the specified PredicateCondition");
  }
}

std::pair<PredicateCondition, PredicateCondition> between_to_conditions(const PredicateCondition predicate_condition) {
  switch (predicate_condition) {
    case PredicateCondition::BetweenInclusive:
      return {PredicateCondition::GreaterThanEquals, PredicateCondition::LessThanEquals};
    case PredicateCondition::BetweenLowerExclusive:
      return {PredicateCondition::GreaterThan, PredicateCondition::LessThanEquals};
    case PredicateCondition::BetweenUpperExclusive:
      return {PredicateCondition::GreaterThanEquals, PredicateCondition::LessThan};
    case PredicateCondition::BetweenExclusive:
      return {PredicateCondition::GreaterThan, PredicateCondition::LessThan};
    default:
      Fail("Input was not a between condition");
  }
}

PredicateCondition conditions_to_between(const PredicateCondition lower, const PredicateCondition upper) {
  if (lower == PredicateCondition::GreaterThan) {
    if (upper == PredicateCondition::LessThan) {
      return PredicateCondition::BetweenExclusive;
    }

    if (upper == PredicateCondition::LessThanEquals) {
      return PredicateCondition::BetweenLowerExclusive;
    }
  } else if (lower == PredicateCondition::GreaterThanEquals) {
    if (upper == PredicateCondition::LessThan) {
      return PredicateCondition::BetweenUpperExclusive;
    }

    if (upper == PredicateCondition::LessThanEquals) {
      return PredicateCondition::BetweenInclusive;
    }
  }
  Fail("Unexpected PredicateCondition");
}

std::ostream& operator<<(std::ostream& stream, PredicateCondition predicate_condition) {
  static const auto predicate_condition_to_string = std::unordered_map<PredicateCondition, std::string>({
      {PredicateCondition::Equals, "="},
      {PredicateCondition::NotEquals, "!="},
      {PredicateCondition::LessThan, "<"},
      {PredicateCondition::LessThanEquals, "<="},
      {PredicateCondition::GreaterThan, ">"},
      {PredicateCondition::GreaterThanEquals, ">="},
      {PredicateCondition::BetweenInclusive, "BETWEEN INCLUSIVE"},
      {PredicateCondition::BetweenLowerExclusive, "BETWEEN LOWER EXCLUSIVE"},
      {PredicateCondition::BetweenUpperExclusive, "BETWEEN UPPER EXCLUSIVE"},
      {PredicateCondition::BetweenExclusive, "BETWEEN EXCLUSIVE"},
      {PredicateCondition::Like, "LIKE"},
      {PredicateCondition::NotLike, "NOT LIKE"},
      {PredicateCondition::In, "IN"},
      {PredicateCondition::NotIn, "NOT IN"},
      {PredicateCondition::IsNull, "IS NULL"},
      {PredicateCondition::IsNotNull, "IS NOT NULL"},
  });

  return stream << predicate_condition_to_string.at(predicate_condition);
}

std::ostream& operator<<(std::ostream& stream, SortMode sort_mode) {
  return stream << magic_enum::enum_name(sort_mode);
}

std::ostream& operator<<(std::ostream& stream, JoinMode join_mode) {
  return stream << magic_enum::enum_name(join_mode);
}

std::ostream& operator<<(std::ostream& stream, SetOperationMode set_operation_mode) {
  return stream << magic_enum::enum_name(set_operation_mode);
}

std::ostream& operator<<(std::ostream& stream, TableType table_type) {
  return stream << magic_enum::enum_name(table_type);
}

}  // namespace hyrise
