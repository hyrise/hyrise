#include "gtest/gtest.h"

#include "utils/lossless_predicate_cast.hpp"

namespace opossum {

class LosslessPredicateCastTest : public ::testing::Test {};

TEST_F(LosslessPredicateCastTest, NextFloatTowards) {
  // 3 is directly representable as a float
  EXPECT_EQ(*next_float_towards(3, 2), 2.9999997615814208984375f);
  EXPECT_EQ(*next_float_towards(3, 4), 3.0000002384185791015625f);

  // 3.1 is not:
  EXPECT_EQ(*next_float_towards(3.1, 3), 3.099999904632568359375f);
  EXPECT_EQ(*next_float_towards(3.1, 4), 3.1000001430511474609375f);

  // Edge cases:
  EXPECT_EQ(next_float_towards(3.1, 3.1), std::nullopt);

  // // Maximum double that can losslessly represented as a float and the next higher double
  EXPECT_EQ(*next_float_towards(340282346638528859811704183484516925440.0, 0),
            340282326356119256160033759537265639424.0f);
  EXPECT_EQ(
      next_float_towards(340282346638528859811704183484516925440.0, 340282346638528859811704183484516925440.0 * 10),
      std::nullopt);
  EXPECT_EQ(next_float_towards(340282346638528897590636046441678635008.0, 0), std::nullopt);
  EXPECT_EQ(
      next_float_towards(340282346638528897590636046441678635008.0, 340282346638528897590636046441678635008.0 * 10),
      std::nullopt);

  // Minimum double that can losslessly represented as a float and the next lower double
  EXPECT_EQ(*next_float_towards(-340282346638528859811704183484516925440.0, -10),
            -340282326356119256160033759537265639424.0f);
  EXPECT_EQ(
      next_float_towards(-340282346638528859811704183484516925440.0, -340282346638528859811704183484516925440.0 * 10),
      std::nullopt);
  EXPECT_EQ(next_float_towards(-340282346638528897590636046441678635008.0, 10), std::nullopt);
  EXPECT_EQ(
      next_float_towards(-340282346638528897590636046441678635008.0, -340282346638528897590636046441678635008.0 * 10),
      std::nullopt);
}

TEST_F(LosslessPredicateCastTest, NonFloatTypes) {
  // Needed because the C precompiler does not understand templates
  using Int32Result = std::pair<PredicateCondition, int32_t>;
  using Int64Result = std::pair<PredicateCondition, int64_t>;
  using StringResult = std::pair<PredicateCondition, pmr_string>;

  // Input type == output type
  EXPECT_EQ(*lossless_predicate_cast<int64_t>(PredicateCondition::GreaterThan, int64_t{10}),
            Int64Result(PredicateCondition::GreaterThan, int64_t{10}));
  EXPECT_EQ(*lossless_predicate_cast<int64_t>(PredicateCondition::Equals, int64_t{10}),
            Int64Result(PredicateCondition::Equals, int64_t{10}));
  EXPECT_EQ(*lossless_predicate_cast<pmr_string>(PredicateCondition::Like, pmr_string{"foo"}),
            StringResult(PredicateCondition::Like, pmr_string{"foo"}));

  // Downcast
  EXPECT_EQ(*lossless_predicate_cast<int32_t>(PredicateCondition::GreaterThan, int64_t{10}),
            Int32Result(PredicateCondition::GreaterThan, int32_t{10}));
  EXPECT_EQ(lossless_predicate_cast<int32_t>(PredicateCondition::GreaterThan, int64_t{100'000'000'000}), std::nullopt);

  // Upcast
  EXPECT_EQ(*lossless_predicate_cast<int64_t>(PredicateCondition::GreaterThan, int32_t{10}),
            Int64Result(PredicateCondition::GreaterThan, int64_t{10}));
}

TEST_F(LosslessPredicateCastTest, FloatTypeWithLosslessCast) {
  using FloatResult = std::pair<PredicateCondition, float>;

  EXPECT_EQ(*lossless_predicate_cast<float>(PredicateCondition::GreaterThan, 3.0),
            FloatResult(PredicateCondition::GreaterThan, 3.f));
}

TEST_F(LosslessPredicateCastTest, FloatTypeWithAdjustedValues) {
  using FloatResult = std::pair<PredicateCondition, float>;

  EXPECT_EQ(*lossless_predicate_cast<float>(PredicateCondition::LessThan, 3.1),
            FloatResult(PredicateCondition::LessThanEquals, 3.099999904632568359375f));
  EXPECT_EQ(*lossless_predicate_cast<float>(PredicateCondition::LessThanEquals, 3.1),
            FloatResult(PredicateCondition::LessThanEquals, 3.099999904632568359375f));
  EXPECT_EQ(lossless_predicate_cast<float>(PredicateCondition::Equals, 3.1), std::nullopt);
  EXPECT_EQ(*lossless_predicate_cast<float>(PredicateCondition::GreaterThan, 3.1),
            FloatResult(PredicateCondition::GreaterThanEquals, 3.1000001430511474609375f));
  EXPECT_EQ(*lossless_predicate_cast<float>(PredicateCondition::GreaterThanEquals, 3.1),
            FloatResult(PredicateCondition::GreaterThanEquals, 3.1000001430511474609375f));
}

}  // namespace opossum
