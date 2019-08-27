#include "types.hpp"

#include "utils/make_bimap.hpp"

namespace opossum {

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
    } else if (upper == PredicateCondition::LessThanEquals) {
      return PredicateCondition::BetweenLowerExclusive;
    }
  } else if (lower == PredicateCondition::GreaterThanEquals) {
    if (upper == PredicateCondition::LessThan) {
      return PredicateCondition::BetweenUpperExclusive;
    } else if (upper == PredicateCondition::LessThanEquals) {
      return PredicateCondition::BetweenInclusive;
    }
  }
  Fail("Unexpected PredicateCondition");
}

const boost::bimap<PredicateCondition, std::string> predicate_condition_to_string =
    make_bimap<PredicateCondition, std::string>({
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

const boost::bimap<OrderByMode, std::string> order_by_mode_to_string = make_bimap<OrderByMode, std::string>({
    {OrderByMode::Ascending, "AscendingNullsFirst"},
    {OrderByMode::Descending, "DescendingNullsFirst"},
    {OrderByMode::AscendingNullsLast, "AscendingNullsLast"},
    {OrderByMode::DescendingNullsLast, "DescendingNullsLast"},
});

const boost::bimap<JoinMode, std::string> join_mode_to_string = make_bimap<JoinMode, std::string>({
    {JoinMode::Cross, "Cross"},
    {JoinMode::Inner, "Inner"},
    {JoinMode::Left, "Left"},
    {JoinMode::FullOuter, "FullOuter"},
    {JoinMode::Right, "Right"},
    {JoinMode::Semi, "Semi"},
    {JoinMode::AntiNullAsTrue, "AntiNullAsTrue"},
    {JoinMode::AntiNullAsFalse, "AntiNullAsFalse"},
});

const boost::bimap<TableType, std::string> table_type_to_string =
    make_bimap<TableType, std::string>({{TableType::Data, "Data"}, {TableType::References, "References"}});

const boost::bimap<UnionMode, std::string> union_mode_to_string =
    make_bimap<UnionMode, std::string>({{UnionMode::All, "UnionAll"}, {UnionMode::Positions, "UnionPositions"}});

std::ostream& operator<<(std::ostream& stream, PredicateCondition predicate_condition) {
  return stream << predicate_condition_to_string.left.at(predicate_condition);
}

std::ostream& operator<<(std::ostream& stream, OrderByMode order_by_mode) {
  return stream << order_by_mode_to_string.left.at(order_by_mode);
}

std::ostream& operator<<(std::ostream& stream, JoinMode join_mode) {
  return stream << join_mode_to_string.left.at(join_mode);
}

std::ostream& operator<<(std::ostream& stream, UnionMode union_mode) {
  return stream << union_mode_to_string.left.at(union_mode);
}

std::ostream& operator<<(std::ostream& stream, TableType table_type) {
  return stream << table_type_to_string.left.at(table_type);
}

}  // namespace opossum
