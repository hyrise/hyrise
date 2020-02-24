#include <algorithm>
#include <functional>
#include <limits>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "base_test.hpp"

#include "statistics/statistics_objects/min_max_filter.hpp"
#include "statistics/statistics_objects/range_filter.hpp"
#include "types.hpp"
#include "utils/assert.hpp"

namespace opossum {

template <typename T>
class RangeFilterTest : public BaseTest {
 protected:
  void SetUp() override {
    // Manually created vector. Largest exlusive gap (only gap when gap_count == 1) will
    // be 103-123456, second largest -1000 to 2, third 17-100.
    _values = pmr_vector<T>{-1000, 2, 3, 4, 7, 8, 10, 17, 100, 101, 102, 103, 123456};

    _min_value = *std::min_element(std::begin(_values), std::end(_values));
    _max_value = *std::max_element(std::begin(_values), std::end(_values));

    // `_value_in_gap` in a value in the largest gap of the test data.
    _value_in_gap = T{1024};
    _value_smaller_than_minimum = _min_value - 1;  // value smaller than the minimum
    _value_larger_than_maximum = _max_value + 1;   // value larger than the maximum
  }

  pmr_vector<T> _values;
  T _value_smaller_than_minimum, _min_value, _max_value, _value_larger_than_maximum, _value_in_gap;
};

using RangeFilterTypes = ::testing::Types<int, float, double>;
TYPED_TEST_SUITE(RangeFilterTest, RangeFilterTypes, );  // NOLINT(whitespace/parens)

TYPED_TEST(RangeFilterTest, ValueRangeTooLarge) {
  const auto lowest = std::numeric_limits<TypeParam>::lowest();
  const auto max = std::numeric_limits<TypeParam>::max();
  // Create vector with a huge gap in the middle whose length exceeds the type's limits.
  const pmr_vector<TypeParam> test_vector{static_cast<TypeParam>(0.9 * lowest), static_cast<TypeParam>(0.8 * lowest),
                                          static_cast<TypeParam>(0.8 * max), static_cast<TypeParam>(0.9 * max)};

  // The filter will not create 5 ranges due to potential overflow problems when calculating
  // distances. In this case, only a filter with a single range is built.
  auto filter = RangeFilter<TypeParam>::build_filter(test_vector, 5);
  // Having only one range means the filter cannot prune 0 right in the largest gap.
  EXPECT_FALSE(filter->does_not_contain(PredicateCondition::Equals, TypeParam{0}));

  // Nonetheless, the filter should prune values outside the single range.
  EXPECT_TRUE(filter->does_not_contain(PredicateCondition::Equals, static_cast<TypeParam>(lowest * 0.95)));
}

TYPED_TEST(RangeFilterTest, ThrowOnUnsortedData) {
  if (!HYRISE_DEBUG) GTEST_SKIP();

  const pmr_vector<TypeParam> test_vector{std::numeric_limits<TypeParam>::max(),
                                          std::numeric_limits<TypeParam>::lowest()};

  // Additional parantheses needed for template macro expansion.
  EXPECT_THROW((RangeFilter<TypeParam>::build_filter(test_vector, 5)), std::logic_error);
}

// a single range is basically a min/max filter
TYPED_TEST(RangeFilterTest, SingleRange) {
  const auto filter = RangeFilter<TypeParam>::build_filter(this->_values, 1);

  for (const auto& value : this->_values) {
    EXPECT_FALSE(filter->does_not_contain(PredicateCondition::Equals, {value}));
  }

  // testing for interval bounds
  EXPECT_TRUE(filter->does_not_contain(PredicateCondition::LessThan, {this->_min_value}));
  EXPECT_FALSE(filter->does_not_contain(PredicateCondition::GreaterThan, {this->_min_value}));

  // cannot prune values in between, even though non-existent
  EXPECT_FALSE(filter->does_not_contain(PredicateCondition::Equals, TypeParam{this->_value_in_gap}));

  EXPECT_TRUE(filter->does_not_contain(PredicateCondition::BetweenInclusive, TypeParam{-3000}, TypeParam{-2000}));

  EXPECT_FALSE(filter->does_not_contain(PredicateCondition::LessThanEquals, {this->_max_value}));
  EXPECT_TRUE(filter->does_not_contain(PredicateCondition::GreaterThan, {this->_max_value}));
}

// create range filters with varying number of ranges/gaps
TYPED_TEST(RangeFilterTest, MultipleRanges) {
  const auto first_gap_min = TypeParam{104};
  const auto first_gap_max = TypeParam{123455};

  const auto second_gap_min = TypeParam{-999};
  const auto second_gap_max = TypeParam{1};

  const auto third_gap_min = TypeParam{18};
  const auto third_gap_max = TypeParam{99};

  {
    const auto filter = RangeFilter<TypeParam>::build_filter(this->_values, 2);
    EXPECT_TRUE(filter->does_not_contain(PredicateCondition::Equals, this->_value_in_gap));
    EXPECT_TRUE(filter->does_not_contain(PredicateCondition::Equals, first_gap_min));
    EXPECT_TRUE(filter->does_not_contain(PredicateCondition::BetweenInclusive, first_gap_min, first_gap_max));

    EXPECT_FALSE(filter->does_not_contain(PredicateCondition::BetweenInclusive, second_gap_min, second_gap_max));
    EXPECT_FALSE(filter->does_not_contain(PredicateCondition::BetweenInclusive, third_gap_min, third_gap_max));
  }
  {
    const auto filter = RangeFilter<TypeParam>::build_filter(this->_values, 3);
    EXPECT_TRUE(filter->does_not_contain(PredicateCondition::Equals, this->_value_in_gap));
    EXPECT_TRUE(filter->does_not_contain(PredicateCondition::Equals, first_gap_min));
    EXPECT_TRUE(filter->does_not_contain(PredicateCondition::BetweenInclusive, first_gap_min, first_gap_max));
    EXPECT_TRUE(filter->does_not_contain(PredicateCondition::Equals, second_gap_min));
    EXPECT_TRUE(filter->does_not_contain(PredicateCondition::BetweenInclusive, second_gap_min, second_gap_max));

    EXPECT_FALSE(filter->does_not_contain(PredicateCondition::BetweenInclusive, third_gap_min, third_gap_max));
  }
  // starting with 4 ranges, all tested gaps should be covered
  for (auto range_count : {4, 5, 100, 1'000}) {
    {
      const auto filter = RangeFilter<TypeParam>::build_filter(this->_values, range_count);
      EXPECT_TRUE(filter->does_not_contain(PredicateCondition::Equals, this->_value_in_gap));
      EXPECT_TRUE(filter->does_not_contain(PredicateCondition::Equals, first_gap_min));
      EXPECT_TRUE(filter->does_not_contain(PredicateCondition::BetweenInclusive, first_gap_min, first_gap_max));
      EXPECT_TRUE(filter->does_not_contain(PredicateCondition::Equals, second_gap_min));
      EXPECT_TRUE(filter->does_not_contain(PredicateCondition::BetweenInclusive, second_gap_min, second_gap_max));
      EXPECT_TRUE(filter->does_not_contain(PredicateCondition::Equals, third_gap_min));
      EXPECT_TRUE(filter->does_not_contain(PredicateCondition::BetweenInclusive, third_gap_min, third_gap_max));
    }
  }
  {
    if (!HYRISE_DEBUG) GTEST_SKIP();

    // Throw when range filter shall include 0 range values.
    EXPECT_THROW((RangeFilter<TypeParam>::build_filter(this->_values, 0)), std::logic_error);
  }
}

// create more ranges than distinct values in the test data
TYPED_TEST(RangeFilterTest, MoreRangesThanValues) {
  const auto filter = RangeFilter<TypeParam>::build_filter(this->_values, 10'000);

  for (const auto& value : this->_values) {
    EXPECT_FALSE(filter->does_not_contain(PredicateCondition::Equals, {value}));
  }

  // testing for interval bounds
  EXPECT_TRUE(filter->does_not_contain(PredicateCondition::LessThan, TypeParam{this->_min_value}));
  EXPECT_FALSE(filter->does_not_contain(PredicateCondition::GreaterThan, TypeParam{this->_min_value}));
  EXPECT_TRUE(filter->does_not_contain(PredicateCondition::Equals, TypeParam{this->_value_in_gap}));
  EXPECT_FALSE(filter->does_not_contain(PredicateCondition::LessThanEquals, TypeParam{this->_max_value}));
  EXPECT_TRUE(filter->does_not_contain(PredicateCondition::GreaterThan, TypeParam{this->_max_value}));
}

// this test checks the correct pruning on the bounds (min/max) of the test data for various predicate conditions
// for better understanding, see min_max_filter_test.cpp
TYPED_TEST(RangeFilterTest, CanPruneOnBounds) {
  const auto filter = RangeFilter<TypeParam>::build_filter(this->_values);

  for (const auto& value : this->_values) {
    EXPECT_FALSE(filter->does_not_contain(PredicateCondition::Equals, {value}));
  }

  EXPECT_TRUE(filter->does_not_contain(PredicateCondition::LessThan, {this->_value_smaller_than_minimum}));
  EXPECT_TRUE(filter->does_not_contain(PredicateCondition::LessThan, {this->_min_value}));
  EXPECT_FALSE(filter->does_not_contain(PredicateCondition::LessThan, {this->_value_in_gap}));
  EXPECT_FALSE(filter->does_not_contain(PredicateCondition::LessThan, {this->_max_value}));
  EXPECT_FALSE(filter->does_not_contain(PredicateCondition::LessThan, {this->_value_larger_than_maximum}));

  EXPECT_TRUE(filter->does_not_contain(PredicateCondition::LessThanEquals, {this->_value_smaller_than_minimum}));
  EXPECT_FALSE(filter->does_not_contain(PredicateCondition::LessThanEquals, {this->_min_value}));
  EXPECT_FALSE(filter->does_not_contain(PredicateCondition::LessThanEquals, {this->_value_in_gap}));
  EXPECT_FALSE(filter->does_not_contain(PredicateCondition::LessThanEquals, {this->_max_value}));
  EXPECT_FALSE(filter->does_not_contain(PredicateCondition::LessThanEquals, {this->_value_larger_than_maximum}));

  EXPECT_TRUE(filter->does_not_contain(PredicateCondition::Equals, {this->_value_smaller_than_minimum}));
  EXPECT_FALSE(filter->does_not_contain(PredicateCondition::Equals, {this->_min_value}));
  EXPECT_TRUE(filter->does_not_contain(PredicateCondition::Equals, {this->_value_in_gap}));
  EXPECT_FALSE(filter->does_not_contain(PredicateCondition::Equals, {this->_max_value}));
  EXPECT_TRUE(filter->does_not_contain(PredicateCondition::Equals, {this->_value_larger_than_maximum}));

  EXPECT_FALSE(filter->does_not_contain(PredicateCondition::GreaterThanEquals, {this->_value_smaller_than_minimum}));
  EXPECT_FALSE(filter->does_not_contain(PredicateCondition::GreaterThanEquals, {this->_min_value}));
  EXPECT_FALSE(filter->does_not_contain(PredicateCondition::GreaterThanEquals, {this->_value_in_gap}));
  EXPECT_FALSE(filter->does_not_contain(PredicateCondition::GreaterThanEquals, {this->_max_value}));
  EXPECT_TRUE(filter->does_not_contain(PredicateCondition::GreaterThanEquals, {this->_value_larger_than_maximum}));

  EXPECT_FALSE(filter->does_not_contain(PredicateCondition::GreaterThan, {this->_value_smaller_than_minimum}));
  EXPECT_FALSE(filter->does_not_contain(PredicateCondition::GreaterThan, {this->_min_value}));
  EXPECT_FALSE(filter->does_not_contain(PredicateCondition::GreaterThan, {this->_value_in_gap}));
  EXPECT_TRUE(filter->does_not_contain(PredicateCondition::GreaterThan, {this->_max_value}));
  EXPECT_TRUE(filter->does_not_contain(PredicateCondition::GreaterThan, {this->_value_larger_than_maximum}));
}

// Test larger value ranges.
TYPED_TEST(RangeFilterTest, Between) {
  const auto filter = RangeFilter<TypeParam>::build_filter(this->_values);

  // This one has bounds in gaps, but cannot prune.
  EXPECT_FALSE(filter->does_not_contain(PredicateCondition::BetweenInclusive, {this->_max_value - 1},
                                        {this->_value_larger_than_maximum}));

  EXPECT_TRUE(filter->does_not_contain(PredicateCondition::BetweenInclusive, TypeParam{-3000}, TypeParam{-2000}));
  EXPECT_TRUE(filter->does_not_contain(PredicateCondition::BetweenInclusive, TypeParam{-999}, TypeParam{1}));
  EXPECT_TRUE(filter->does_not_contain(PredicateCondition::BetweenInclusive, TypeParam{104}, TypeParam{1004}));
  EXPECT_TRUE(
      filter->does_not_contain(PredicateCondition::BetweenInclusive, TypeParam{10'000'000}, TypeParam{20'000'000}));

  EXPECT_FALSE(filter->does_not_contain(PredicateCondition::BetweenInclusive, TypeParam{-3000}, TypeParam{-500}));
  EXPECT_FALSE(filter->does_not_contain(PredicateCondition::BetweenInclusive, TypeParam{101}, TypeParam{103}));
  EXPECT_FALSE(filter->does_not_contain(PredicateCondition::BetweenInclusive, TypeParam{102}, TypeParam{1004}));

  // SQL's between is inclusive
  EXPECT_FALSE(filter->does_not_contain(PredicateCondition::BetweenInclusive, TypeParam{103}, TypeParam{123456}));

  // TODO(bensk1): as soon as non-inclusive between predicates are implemented, testing
  // a non-inclusive between with the bounds exactly on the value bounds would be humongous:
  //  EXPECT_TRUE(filter->does_not_contain(PredicateCondition::BetweenNONINCLUSIVE, TypeParam{103}, TypeParam{123456}));
}

// Test larger value ranges.
TYPED_TEST(RangeFilterTest, LargeValueRange) {
  const auto lowest = std::numeric_limits<TypeParam>::lowest();
  const auto max = std::numeric_limits<TypeParam>::max();

  const pmr_vector<TypeParam> values{static_cast<TypeParam>(0.4 * lowest),  static_cast<TypeParam>(0.38 * lowest),
                                     static_cast<TypeParam>(0.36 * lowest), static_cast<TypeParam>(0.30 * lowest),
                                     static_cast<TypeParam>(0.28 * lowest), static_cast<TypeParam>(0.36 * max),
                                     static_cast<TypeParam>(0.38 * max),    static_cast<TypeParam>(0.4 * max)};

  const auto filter = RangeFilter<TypeParam>::build_filter(values, 3);

  // A filter with 3 ranges, has two gaps: (i) 0.28*lowest-0.36*max and (ii) 0.36*lowest-0.30*lowest
  EXPECT_TRUE(filter->does_not_contain(PredicateCondition::BetweenInclusive, static_cast<TypeParam>(0.27 * lowest),
                                       static_cast<TypeParam>(0.35 * max)));
  EXPECT_TRUE(filter->does_not_contain(PredicateCondition::BetweenInclusive, static_cast<TypeParam>(0.35 * lowest),
                                       static_cast<TypeParam>(0.31 * lowest)));

  EXPECT_TRUE(filter->does_not_contain(PredicateCondition::Equals, static_cast<TypeParam>(TypeParam{0})));  // in gap
  EXPECT_TRUE(filter->does_not_contain(PredicateCondition::Equals, static_cast<TypeParam>(0.5 * lowest)));
  EXPECT_TRUE(filter->does_not_contain(PredicateCondition::Equals, static_cast<TypeParam>(0.5 * max)));

  EXPECT_FALSE(filter->does_not_contain(PredicateCondition::Equals, static_cast<TypeParam>(values.front()),
                                        static_cast<TypeParam>(values[4])));
  EXPECT_FALSE(filter->does_not_contain(PredicateCondition::Equals, static_cast<TypeParam>(values[5]),
                                        static_cast<TypeParam>(values.back())));

  // As SQL-between is inclusive, this range cannot be pruned.
  EXPECT_FALSE(filter->does_not_contain(PredicateCondition::Equals, static_cast<TypeParam>(values[4]),
                                        static_cast<TypeParam>(values[5])));

  EXPECT_FALSE(filter->does_not_contain(PredicateCondition::Equals, static_cast<TypeParam>(0.4 * lowest)));
  EXPECT_FALSE(filter->does_not_contain(PredicateCondition::Equals, static_cast<TypeParam>(0.4 * max)));

  // With two gaps, the following should not exist.
  EXPECT_FALSE(filter->does_not_contain(PredicateCondition::BetweenInclusive, static_cast<TypeParam>(0.4 * lowest),
                                        static_cast<TypeParam>(0.38 * lowest)));
}

TYPED_TEST(RangeFilterTest, Sliced) {
  using Ranges = std::vector<std::pair<TypeParam, TypeParam>>;

  auto new_filter = std::shared_ptr<RangeFilter<TypeParam>>{};
  const auto ranges = std::vector<std::pair<TypeParam, TypeParam>>{{5, 10}, {20, 25}, {35, 100}};

  const auto filter = std::make_shared<RangeFilter<TypeParam>>(ranges);

  new_filter =
      std::static_pointer_cast<RangeFilter<TypeParam>>(filter->sliced(PredicateCondition::NotEquals, TypeParam{7}));
  EXPECT_EQ(new_filter->ranges, Ranges({{5, 10}, {20, 25}, {35, 100}}));

  new_filter = std::static_pointer_cast<RangeFilter<TypeParam>>(
      filter->sliced(PredicateCondition::LessThanEquals, TypeParam{7}));
  EXPECT_EQ(new_filter->ranges, Ranges({{5, 7}}));

  new_filter = std::static_pointer_cast<RangeFilter<TypeParam>>(
      filter->sliced(PredicateCondition::LessThanEquals, TypeParam{17}));
  EXPECT_EQ(new_filter->ranges, Ranges({{5, 10}}));

  new_filter = std::static_pointer_cast<RangeFilter<TypeParam>>(
      filter->sliced(PredicateCondition::GreaterThanEquals, TypeParam{7}));
  EXPECT_EQ(new_filter->ranges, Ranges({{7, 10}, {20, 25}, {35, 100}}));

  new_filter = std::static_pointer_cast<RangeFilter<TypeParam>>(
      filter->sliced(PredicateCondition::GreaterThanEquals, TypeParam{17}));
  EXPECT_EQ(new_filter->ranges, Ranges({{20, 25}, {35, 100}}));

  new_filter = std::static_pointer_cast<RangeFilter<TypeParam>>(
      filter->sliced(PredicateCondition::BetweenInclusive, TypeParam{7}, TypeParam{17}));
  EXPECT_EQ(new_filter->ranges, Ranges({{7, 10}}));

  // New filter should start at 7 and end right before first gap (because 17 is in that gap).

  new_filter = std::static_pointer_cast<RangeFilter<TypeParam>>(
      filter->sliced(PredicateCondition::BetweenInclusive, TypeParam{17}, TypeParam{27}));
  EXPECT_EQ(new_filter->ranges, Ranges({{20, 25}}));

  // Slice with equality predicate will return MinMaxFilter.
  const auto min_max_filter =
      std::dynamic_pointer_cast<MinMaxFilter<TypeParam>>(filter->sliced(PredicateCondition::Equals, TypeParam{7}));
  EXPECT_EQ(min_max_filter->min, 7);
  EXPECT_EQ(min_max_filter->max, 7);
}

TYPED_TEST(RangeFilterTest, SliceWithPredicateReturnsNullptr) {
  const auto filter = RangeFilter<TypeParam>::build_filter(this->_values, 5);

  EXPECT_EQ(filter->sliced(PredicateCondition::LessThan, this->_min_value), nullptr);
  EXPECT_NE(filter->sliced(PredicateCondition::LessThanEquals, this->_min_value), nullptr);
  EXPECT_NE(filter->sliced(PredicateCondition::GreaterThanEquals, this->_max_value), nullptr);
  EXPECT_EQ(filter->sliced(PredicateCondition::GreaterThan, this->_max_value), nullptr);
}

class RangeFilterTestUntyped : public BaseTest {};

// Test predicates which are not supported by the range filter
TEST_F(RangeFilterTestUntyped, DoNotPruneUnsupportedPredicates) {
  const pmr_vector<int> values{-1000, -900, 900, 1000};
  const auto filter = RangeFilter<int>::build_filter(values);

  EXPECT_FALSE(filter->does_not_contain(PredicateCondition::IsNull, {17}));
  EXPECT_FALSE(filter->does_not_contain(PredicateCondition::Like, {17}));
  EXPECT_FALSE(filter->does_not_contain(PredicateCondition::NotLike, {17}));
  EXPECT_FALSE(filter->does_not_contain(PredicateCondition::In, {17}));
  EXPECT_FALSE(filter->does_not_contain(PredicateCondition::NotIn, {17}));
  EXPECT_FALSE(filter->does_not_contain(PredicateCondition::IsNull, {17}));
  EXPECT_FALSE(filter->does_not_contain(PredicateCondition::IsNotNull, {17}));
  EXPECT_FALSE(filter->does_not_contain(PredicateCondition::IsNull, NULL_VALUE));
  EXPECT_FALSE(filter->does_not_contain(PredicateCondition::IsNotNull, NULL_VALUE));

  // For the default filter, the following value is prunable.
  EXPECT_TRUE(filter->does_not_contain(PredicateCondition::Equals, 1));
  // But malformed predicates are skipped intentionally and are thus not prunable
  EXPECT_FALSE(filter->does_not_contain(PredicateCondition::Equals, 1, NULL_VALUE));
}

}  // namespace opossum
