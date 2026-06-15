#include <stdexcept>

#include "base_test.hpp"
#include "types.hpp"

namespace hyrise {

class TypesTest : public BaseTest {};

TEST_F(TypesTest, FlipPredicateCondition) {
  EXPECT_THROW(flip_predicate_condition(PredicateCondition::BetweenInclusive), std::logic_error);
  EXPECT_THROW(flip_predicate_condition(PredicateCondition::BetweenLowerExclusive), std::logic_error);
  EXPECT_THROW(flip_predicate_condition(PredicateCondition::BetweenUpperExclusive), std::logic_error);
  EXPECT_THROW(flip_predicate_condition(PredicateCondition::BetweenExclusive), std::logic_error);
  EXPECT_THROW(flip_predicate_condition(PredicateCondition::In), std::logic_error);
  EXPECT_THROW(flip_predicate_condition(PredicateCondition::NotIn), std::logic_error);
  EXPECT_THROW(flip_predicate_condition(PredicateCondition::Like), std::logic_error);
  EXPECT_THROW(flip_predicate_condition(PredicateCondition::NotLike), std::logic_error);
  EXPECT_THROW(flip_predicate_condition(PredicateCondition::LikeInsensitive), std::logic_error);
  EXPECT_THROW(flip_predicate_condition(PredicateCondition::NotLikeInsensitive), std::logic_error);
  EXPECT_THROW(flip_predicate_condition(PredicateCondition::IsNull), std::logic_error);
  EXPECT_THROW(flip_predicate_condition(PredicateCondition::IsNotNull), std::logic_error);
}

TEST_F(TypesTest, ConditionAndBetweenConversion) {
  EXPECT_THROW(between_to_conditions(PredicateCondition::IsNull), std::logic_error);
  EXPECT_THROW(conditions_to_between(PredicateCondition::IsNull, PredicateCondition::IsNull), std::logic_error);
}

}  // namespace hyrise