#include <algorithm>
#include <functional>
#include <limits>
#include <memory>
#include <random>
#include <set>
#include <string>
#include <utility>
#include <vector>

#include "base_test.hpp"
#include "gtest/gtest.h"

#include "utils/assert.hpp"

#include "statistics/chunk_statistics/min_max_filter.hpp"
#include "statistics/chunk_statistics/range_filter.hpp"
#include "statistics/empty_statistics_object.hpp"
#include "types.hpp"

namespace opossum {

template <typename T>
class RangeFilterTest : public ::testing::Test {
 protected:
  void SetUp() override {
    _values = pmr_vector<T>{-1000, 2, 3, 4, 7, 8, 10, 17, 100, 101, 102, 103, 123456};
    _min_value = *std::min_element(std::begin(_values), std::end(_values));
    _max_value = *std::max_element(std::begin(_values), std::end(_values));

    // `_in_between` in a value in the largest gap of the test data.
    // When test data is changed, ensure that value is not part of a range in ranges unless |ranges| == 1.
    _in_between = static_cast<T>(_min_value + 0.5 * (_max_value - _min_value));

    _before_range = _min_value - 1;  // value smaller than the minimum
    _after_range = _max_value + 1;   // value larger than the maximum
  }

  // values ought to be sorted!
  std::shared_ptr<RangeFilter<T>> test_varying_range_filter_size(const size_t gap_count, const pmr_vector<T>& values) {
    // RangeFilter constructor takes range count, not gap count
    auto filter = RangeFilter<T>::build_filter(values, static_cast<uint32_t>(gap_count + 1));

    for (const auto& value : values) {
      EXPECT_EQ(filter->estimate_cardinality(PredicateCondition::Equals, {value}).type,
                EstimateType::MatchesApproximately);
    }

    // Find `gap_count` largest gaps. We use an std::{{set}} to discard repeated
    // values and directly iterate over them in sorted order.
    const auto value_set = std::set<T>(values.cbegin(), values.cend(), std::less<T>());
    std::vector<std::pair<std::pair<T, T>, T>> interval_length_pairs;

    for (auto it = value_set.begin(); it != std::prev(value_set.end()); ++it) {
      const auto begin = *it;
      const auto end = *(std::next(it));
      interval_length_pairs.push_back({{begin, end}, abs(end - begin)});
    }

    std::sort(interval_length_pairs.begin(), interval_length_pairs.end(),
              [](auto& left, auto& right) { return left.second > right.second; });

    for (auto gap_index = size_t{0}; gap_index < gap_count && gap_index < interval_length_pairs.size(); ++gap_index) {
      const auto gap = interval_length_pairs[gap_index];
      const auto begin = gap.first.first;
      const auto end = gap.first.second;
      const auto length = gap.second;

      // The self-calculated gaps are non-inclusive!
      EXPECT_EQ(filter->estimate_cardinality(PredicateCondition::Equals, {begin}).type,
                EstimateType::MatchesApproximately);
      EXPECT_EQ(filter->estimate_cardinality(PredicateCondition::Equals, {end}).type,
                EstimateType::MatchesApproximately);

      if constexpr (std::numeric_limits<T>::is_iec559) {
        auto value_in_gap = begin + 0.5 * length;
        EXPECT_EQ(filter->estimate_cardinality(PredicateCondition::Equals, {value_in_gap}).type,
                  EstimateType::MatchesNone);
      } else if constexpr (std::is_integral_v<T>) {  // NOLINT
        if (length > 1) {
          EXPECT_EQ(filter->estimate_cardinality(PredicateCondition::Equals, {begin + 1}).type,
                    EstimateType::MatchesNone);
          EXPECT_EQ(filter->estimate_cardinality(PredicateCondition::Equals, {end - 1}).type,
                    EstimateType::MatchesNone);
        }
      }
    }

    // _in_between should always prune if we have more than one range
    if (gap_count > 1) {
      EXPECT_EQ(filter->estimate_cardinality(PredicateCondition::Equals, _in_between).type, EstimateType::MatchesNone);
    }
    return filter;
  }

  pmr_vector<T> _values;
  T _before_range, _min_value, _max_value, _after_range, _in_between;
};

template <typename T>
using distribution =
    std::conditional_t<std::is_integral<T>::value, std::uniform_int_distribution<T>,
                       std::conditional_t<std::is_floating_point<T>::value, std::uniform_real_distribution<T>, void>>;

template <typename T>
T get_random_number(std::mt19937& rng, distribution<T> distribution) {
  return distribution(rng);
}

using FilterTypes = ::testing::Types<int, float, double>;
TYPED_TEST_CASE(RangeFilterTest, FilterTypes, );  // NOLINT(whitespace/parens)

TYPED_TEST(RangeFilterTest, ValueRangeTooLarge) {
  // Create vector with a huge gap in the middle whose length exceeds the type's limits.
  const pmr_vector<TypeParam> test_vector{static_cast<TypeParam>(0.9 * std::numeric_limits<TypeParam>::lowest()),
                                          static_cast<TypeParam>(0.8 * std::numeric_limits<TypeParam>::lowest()),
                                          static_cast<TypeParam>(0.8 * std::numeric_limits<TypeParam>::max()),
                                          static_cast<TypeParam>(0.9 * std::numeric_limits<TypeParam>::max())};

  // The filter will not create 5 ranges due to potential overflow problems when calculating
  // distances. In this case, only a filter with a single range is built.
  auto filter = RangeFilter<TypeParam>::build_filter(test_vector, 5);
  // Having only one range means the filter cannot prune 0 right in the largest gap.
  EXPECT_EQ(filter->estimate_cardinality(PredicateCondition::Equals, 0).type, EstimateType::MatchesApproximately);
  // Nonetheless, the filter should prune values outside the single range.
  EXPECT_EQ(
      filter->estimate_cardinality(PredicateCondition::Equals, std::numeric_limits<TypeParam>::lowest() * 0.95).type,
      EstimateType::MatchesNone);
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
    EXPECT_EQ(filter->estimate_cardinality(PredicateCondition::Equals, {value}).type,
              EstimateType::MatchesApproximately);
  }

  // testing for interval bounds
  EXPECT_EQ(filter->estimate_cardinality(PredicateCondition::LessThan, {this->_min_value}).type,
            EstimateType::MatchesNone);
  EXPECT_EQ(filter->estimate_cardinality(PredicateCondition::GreaterThan, {this->_min_value}).type,
            EstimateType::MatchesApproximately);

  // cannot prune values in between, even though non-existent
  EXPECT_EQ(filter->estimate_cardinality(PredicateCondition::Equals, {this->_in_between}).type,
            EstimateType::MatchesApproximately);

  EXPECT_EQ(filter->estimate_cardinality(PredicateCondition::LessThanEquals, {this->_max_value}).type,
            EstimateType::MatchesApproximately);
  EXPECT_EQ(filter->estimate_cardinality(PredicateCondition::GreaterThan, {this->_max_value}).type,
            EstimateType::MatchesNone);
}

// create range filters with varying number of ranges/gaps
TYPED_TEST(RangeFilterTest, MultipleRanges) {
  for (auto gap_count = size_t{0}; gap_count < this->_values.size() * 2; ++gap_count) {
    const auto filter = this->test_varying_range_filter_size(gap_count, this->_values);

    // _in_between should always prune if   we have more than one range
    if (gap_count > 1) {
      EXPECT_EQ(filter->estimate_cardinality(PredicateCondition::Equals, this->_in_between).type,
                EstimateType::MatchesNone);
    }
  }
}

// create more ranges than distinct values in the test data
TYPED_TEST(RangeFilterTest, MoreRangesThanValues) {
  const auto filter = RangeFilter<TypeParam>::build_filter(this->_values, 10'000);

  for (const auto& value : this->_values) {
    EXPECT_EQ(filter->estimate_cardinality(PredicateCondition::Equals, {value}).type,
              EstimateType::MatchesApproximately);
  }

  // testing for interval bounds
  EXPECT_EQ(filter->estimate_cardinality(PredicateCondition::LessThan, {this->_min_value}).type,
            EstimateType::MatchesNone);
  EXPECT_EQ(filter->estimate_cardinality(PredicateCondition::GreaterThan, {this->_min_value}).type,
            EstimateType::MatchesApproximately);
  EXPECT_EQ(filter->estimate_cardinality(PredicateCondition::Equals, {this->_in_between}).type,
            EstimateType::MatchesNone);
  EXPECT_EQ(filter->estimate_cardinality(PredicateCondition::LessThanEquals, {this->_max_value}).type,
            EstimateType::MatchesApproximately);
  EXPECT_EQ(filter->estimate_cardinality(PredicateCondition::GreaterThan, {this->_max_value}).type,
            EstimateType::MatchesNone);
}

// Test predicates which are not supported by the range filter
TYPED_TEST(RangeFilterTest, DoNotPruneUnsupportedPredicates) {
  const auto filter = RangeFilter<TypeParam>::build_filter(this->_values);

  EXPECT_EQ(filter->estimate_cardinality(PredicateCondition::IsNull, {17}).type, EstimateType::MatchesApproximately);
  EXPECT_EQ(filter->estimate_cardinality(PredicateCondition::Like, {17}).type, EstimateType::MatchesApproximately);
  EXPECT_EQ(filter->estimate_cardinality(PredicateCondition::NotLike, {17}).type, EstimateType::MatchesApproximately);
  EXPECT_EQ(filter->estimate_cardinality(PredicateCondition::In, {17}).type, EstimateType::MatchesApproximately);
  EXPECT_EQ(filter->estimate_cardinality(PredicateCondition::NotIn, {17}).type, EstimateType::MatchesApproximately);
  EXPECT_EQ(filter->estimate_cardinality(PredicateCondition::IsNull, {17}).type, EstimateType::MatchesApproximately);
  EXPECT_EQ(filter->estimate_cardinality(PredicateCondition::IsNotNull, {17}).type, EstimateType::MatchesApproximately);
  EXPECT_EQ(filter->estimate_cardinality(PredicateCondition::IsNull, NULL_VALUE).type,
            EstimateType::MatchesApproximately);
  EXPECT_EQ(filter->estimate_cardinality(PredicateCondition::IsNotNull, NULL_VALUE).type,
            EstimateType::MatchesApproximately);

  // For the default filter, the following value is prunable.
  EXPECT_EQ(filter->estimate_cardinality(PredicateCondition::Equals, {this->_in_between}).type,
            EstimateType::MatchesNone);
  // But malformed predicates are skipped intentially and are thus not prunable
  EXPECT_EQ(filter->estimate_cardinality(PredicateCondition::Equals, {this->_in_between}, NULL_VALUE).type,
            EstimateType::MatchesApproximately);
}

// this test checks the correct pruning on the bounds (min/max) of the test data for various predicate conditions
// for better understanding, see min_max_filter_test.cpp
TYPED_TEST(RangeFilterTest, CanPruneOnBounds) {
  const auto filter = RangeFilter<TypeParam>::build_filter(this->_values);

  for (const auto& value : this->_values) {
    EXPECT_EQ(filter->estimate_cardinality(PredicateCondition::Equals, {value}).type,
              EstimateType::MatchesApproximately);
  }

  EXPECT_EQ(filter->estimate_cardinality(PredicateCondition::LessThan, {this->_before_range}).type,
            EstimateType::MatchesNone);
  EXPECT_EQ(filter->estimate_cardinality(PredicateCondition::LessThan, {this->_min_value}).type,
            EstimateType::MatchesNone);
  EXPECT_EQ(filter->estimate_cardinality(PredicateCondition::LessThan, {this->_in_between}).type,
            EstimateType::MatchesApproximately);
  EXPECT_EQ(filter->estimate_cardinality(PredicateCondition::LessThan, {this->_max_value}).type,
            EstimateType::MatchesApproximately);
  EXPECT_EQ(filter->estimate_cardinality(PredicateCondition::LessThan, {this->_after_range}).type,
            EstimateType::MatchesApproximately);

  EXPECT_EQ(filter->estimate_cardinality(PredicateCondition::LessThanEquals, {this->_before_range}).type,
            EstimateType::MatchesNone);
  EXPECT_EQ(filter->estimate_cardinality(PredicateCondition::LessThanEquals, {this->_min_value}).type,
            EstimateType::MatchesApproximately);
  EXPECT_EQ(filter->estimate_cardinality(PredicateCondition::LessThanEquals, {this->_in_between}).type,
            EstimateType::MatchesApproximately);
  EXPECT_EQ(filter->estimate_cardinality(PredicateCondition::LessThanEquals, {this->_max_value}).type,
            EstimateType::MatchesApproximately);
  EXPECT_EQ(filter->estimate_cardinality(PredicateCondition::LessThanEquals, {this->_after_range}).type,
            EstimateType::MatchesApproximately);

  EXPECT_EQ(filter->estimate_cardinality(PredicateCondition::Equals, {this->_before_range}).type,
            EstimateType::MatchesNone);
  EXPECT_EQ(filter->estimate_cardinality(PredicateCondition::Equals, {this->_min_value}).type,
            EstimateType::MatchesApproximately);
  EXPECT_EQ(filter->estimate_cardinality(PredicateCondition::Equals, {this->_in_between}).type,
            EstimateType::MatchesNone);
  EXPECT_EQ(filter->estimate_cardinality(PredicateCondition::Equals, {this->_max_value}).type,
            EstimateType::MatchesApproximately);
  EXPECT_EQ(filter->estimate_cardinality(PredicateCondition::Equals, {this->_after_range}).type,
            EstimateType::MatchesNone);

  EXPECT_EQ(filter->estimate_cardinality(PredicateCondition::GreaterThanEquals, {this->_before_range}).type,
            EstimateType::MatchesApproximately);
  EXPECT_EQ(filter->estimate_cardinality(PredicateCondition::GreaterThanEquals, {this->_min_value}).type,
            EstimateType::MatchesApproximately);
  EXPECT_EQ(filter->estimate_cardinality(PredicateCondition::GreaterThanEquals, {this->_in_between}).type,
            EstimateType::MatchesApproximately);
  EXPECT_EQ(filter->estimate_cardinality(PredicateCondition::GreaterThanEquals, {this->_max_value}).type,
            EstimateType::MatchesApproximately);
  EXPECT_EQ(filter->estimate_cardinality(PredicateCondition::GreaterThanEquals, {this->_after_range}).type,
            EstimateType::MatchesNone);

  EXPECT_EQ(filter->estimate_cardinality(PredicateCondition::GreaterThan, {this->_before_range}).type,
            EstimateType::MatchesApproximately);
  EXPECT_EQ(filter->estimate_cardinality(PredicateCondition::GreaterThan, {this->_min_value}).type,
            EstimateType::MatchesApproximately);
  EXPECT_EQ(filter->estimate_cardinality(PredicateCondition::GreaterThan, {this->_in_between}).type,
            EstimateType::MatchesApproximately);
  EXPECT_EQ(filter->estimate_cardinality(PredicateCondition::GreaterThan, {this->_max_value}).type,
            EstimateType::MatchesNone);
  EXPECT_EQ(filter->estimate_cardinality(PredicateCondition::GreaterThan, {this->_after_range}).type,
            EstimateType::MatchesNone);
}

// Test larger value ranges.
TYPED_TEST(RangeFilterTest, LargeValueRange) {
  std::random_device rd;
  auto rng = std::mt19937(rd());

  // values on which is the range filter is later built on
  pmr_vector<TypeParam> values;
  values.reserve(1'000);

  // Multiplying by 0.6 guarantees that the range filter will always yield a gap range
  // right in the middle around value zero. Note, does not apply for unsigned values.
  const TypeParam lower_bound_low_range = static_cast<TypeParam>(0.4 * std::numeric_limits<TypeParam>::lowest());
  const TypeParam upper_bound_low_range = static_cast<TypeParam>(0.6 * lower_bound_low_range);
  const TypeParam upper_bound_high_range = static_cast<TypeParam>(0.4 * std::numeric_limits<TypeParam>::max());
  const TypeParam lower_bound_high_range = static_cast<TypeParam>(0.6 * upper_bound_high_range);

  // We randomly create values between min_value(TypeParam) to 0.6*min_value(TypeParam) and
  // 0.6*max(TypeParam) to max(TypeParam). Any value in between should be prunable.
  distribution<TypeParam> lower_range_distribution(lower_bound_low_range, upper_bound_low_range);
  distribution<TypeParam> high_range_distribution(lower_bound_high_range, upper_bound_high_range);
  for (auto i = size_t{0}; i < 1'000; ++i) {
    values.push_back(get_random_number<TypeParam>(rng, lower_range_distribution));
    values.push_back(get_random_number<TypeParam>(rng, high_range_distribution));
  }

  std::sort(values.begin(), values.end());

  // For developing reasons, it makes sense to further create range filters with a size of values.size() + 1
  // but we found the runtime to be too long for hyriseTest.
  const std::vector<size_t> gap_counts = {1, 2, 4, 8, 16, 32, 64};
  distribution<TypeParam> middle_gap_distribution(upper_bound_low_range + 1, lower_bound_high_range - 1);
  for (auto gap_count : gap_counts) {
    // execute general tests and receive created range filter
    auto filter = this->test_varying_range_filter_size(gap_count, values);

    // Additionally, test for further values in between the large random value ranges.
    for (auto i = size_t{0}; i < 20; ++i) {
      EXPECT_EQ(filter
                    ->estimate_cardinality(PredicateCondition::Equals,
                                           {get_random_number<TypeParam>(rng, middle_gap_distribution)})
                    .type,
                EstimateType::MatchesNone);
    }
  }
}

/**
 * TODO(moritz) Two tests for BETWEEN. Is this bad?
 */

TYPED_TEST(RangeFilterTest, Between1) {
  const auto ranges = std::vector<std::pair<TypeParam, TypeParam>>{{5, 10}, {20, 25}, {35, 100}};
  const auto filter = std::make_shared<RangeFilter<TypeParam>>(ranges);

  EXPECT_EQ(filter->estimate_cardinality(PredicateCondition::Between, 6, 8).type, EstimateType::MatchesApproximately);
  EXPECT_EQ(filter->estimate_cardinality(PredicateCondition::Between, 6, 12).type, EstimateType::MatchesApproximately);
  EXPECT_EQ(filter->estimate_cardinality(PredicateCondition::Between, 18, 21).type, EstimateType::MatchesApproximately);
  EXPECT_EQ(filter->estimate_cardinality(PredicateCondition::Between, 18, 30).type, EstimateType::MatchesApproximately);

  EXPECT_EQ(filter->estimate_cardinality(PredicateCondition::Between, 100, 0).type, EstimateType::MatchesNone);
  EXPECT_EQ(filter->estimate_cardinality(PredicateCondition::Between, 1, 3).type, EstimateType::MatchesNone);
  EXPECT_EQ(filter->estimate_cardinality(PredicateCondition::Between, 12, 18).type, EstimateType::MatchesNone);
  EXPECT_EQ(filter->estimate_cardinality(PredicateCondition::Between, 110, 200).type, EstimateType::MatchesNone);

  // Bounds
  EXPECT_EQ(filter->estimate_cardinality(PredicateCondition::Between, 1, 4).type, EstimateType::MatchesNone);
  EXPECT_EQ(filter->estimate_cardinality(PredicateCondition::Between, 1, 5).type, EstimateType::MatchesApproximately);
  EXPECT_EQ(filter->estimate_cardinality(PredicateCondition::Between, 10, 12).type, EstimateType::MatchesApproximately);
  EXPECT_EQ(filter->estimate_cardinality(PredicateCondition::Between, 11, 12).type, EstimateType::MatchesNone);
}

// Test larger value ranges.
TYPED_TEST(RangeFilterTest, Between2) {
  const auto filter = RangeFilter<TypeParam>::build_filter(this->_values);

  // This one has bounds in gaps, but cannot prune.
  EXPECT_EQ(
      filter->estimate_cardinality(PredicateCondition::Between, {this->_max_value - 1}, {this->_after_range}).type,
      EstimateType::MatchesApproximately);

  EXPECT_EQ(filter->estimate_cardinality(PredicateCondition::Between, {-3000}, {-2000}).type,
            EstimateType::MatchesNone);
  EXPECT_EQ(filter->estimate_cardinality(PredicateCondition::Between, {-999}, {1}).type, EstimateType::MatchesNone);
  EXPECT_EQ(filter->estimate_cardinality(PredicateCondition::Between, {104}, {1004}).type, EstimateType::MatchesNone);
  EXPECT_EQ(filter->estimate_cardinality(PredicateCondition::Between, {10'000'000}, {20'000'000}).type,
            EstimateType::MatchesNone);

  EXPECT_EQ(filter->estimate_cardinality(PredicateCondition::Between, {-3000}, {-500}).type,
            EstimateType::MatchesApproximately);
  EXPECT_EQ(filter->estimate_cardinality(PredicateCondition::Between, {101}, {103}).type,
            EstimateType::MatchesApproximately);
  EXPECT_EQ(filter->estimate_cardinality(PredicateCondition::Between, {102}, {1004}).type,
            EstimateType::MatchesApproximately);

  // SQL's between is inclusive
  EXPECT_EQ(filter->estimate_cardinality(PredicateCondition::Between, {103}, {123456}).type,
            EstimateType::MatchesApproximately);
}

TYPED_TEST(RangeFilterTest, SliceWithPredicate) {
  auto new_filter = std::shared_ptr<RangeFilter<TypeParam>>{};
  const auto ranges = std::vector<std::pair<TypeParam, TypeParam>>{{5, 10}, {20, 25}, {35, 100}};

  const auto filter = std::make_shared<RangeFilter<TypeParam>>(ranges);
  EXPECT_EQ(filter->estimate_cardinality(PredicateCondition::LessThan, ranges.front().first).type,
            EstimateType::MatchesNone);
  EXPECT_EQ(filter->estimate_cardinality(PredicateCondition::LessThanEquals, ranges.front().first).type,
            EstimateType::MatchesApproximately);
  EXPECT_EQ(filter->estimate_cardinality(PredicateCondition::Equals, 15).type, EstimateType::MatchesNone);
  EXPECT_EQ(filter->estimate_cardinality(PredicateCondition::Equals, 30).type, EstimateType::MatchesNone);
  EXPECT_EQ(filter->estimate_cardinality(PredicateCondition::GreaterThanEquals, ranges.back().second).type,
            EstimateType::MatchesApproximately);
  EXPECT_EQ(filter->estimate_cardinality(PredicateCondition::GreaterThan, ranges.back().second).type,
            EstimateType::MatchesNone);

  new_filter =
      std::static_pointer_cast<RangeFilter<TypeParam>>(filter->slice_with_predicate(PredicateCondition::NotEquals, 7));
  // Should be the same filter.
  EXPECT_EQ(new_filter->estimate_cardinality(PredicateCondition::LessThan, ranges.front().first).type,
            EstimateType::MatchesNone);
  EXPECT_EQ(new_filter->estimate_cardinality(PredicateCondition::LessThanEquals, ranges.front().first).type,
            EstimateType::MatchesApproximately);
  EXPECT_EQ(filter->estimate_cardinality(PredicateCondition::Equals, 15).type, EstimateType::MatchesNone);
  EXPECT_EQ(filter->estimate_cardinality(PredicateCondition::Equals, 30).type, EstimateType::MatchesNone);
  EXPECT_EQ(new_filter->estimate_cardinality(PredicateCondition::GreaterThanEquals, ranges.back().second).type,
            EstimateType::MatchesApproximately);
  EXPECT_EQ(new_filter->estimate_cardinality(PredicateCondition::GreaterThan, ranges.back().second).type,
            EstimateType::MatchesNone);

  new_filter = std::static_pointer_cast<RangeFilter<TypeParam>>(
      filter->slice_with_predicate(PredicateCondition::LessThanEquals, 7));
  // New filter should start at same value as before and end at 7.
  EXPECT_EQ(new_filter->estimate_cardinality(PredicateCondition::LessThan, ranges.front().first).type,
            EstimateType::MatchesNone);
  EXPECT_EQ(new_filter->estimate_cardinality(PredicateCondition::LessThanEquals, ranges.front().first).type,
            EstimateType::MatchesApproximately);
  EXPECT_EQ(new_filter->estimate_cardinality(PredicateCondition::GreaterThanEquals, 7).type,
            EstimateType::MatchesApproximately);
  EXPECT_EQ(new_filter->estimate_cardinality(PredicateCondition::GreaterThan, 7).type, EstimateType::MatchesNone);

  new_filter = std::static_pointer_cast<RangeFilter<TypeParam>>(
      filter->slice_with_predicate(PredicateCondition::LessThanEquals, 17));
  // New filter should start at same value as before and end before first gap (because 17 is in that first gap).
  EXPECT_EQ(new_filter->estimate_cardinality(PredicateCondition::LessThan, ranges.front().first).type,
            EstimateType::MatchesNone);
  EXPECT_EQ(new_filter->estimate_cardinality(PredicateCondition::LessThanEquals, ranges.front().first).type,
            EstimateType::MatchesApproximately);
  EXPECT_EQ(new_filter->estimate_cardinality(PredicateCondition::GreaterThanEquals, ranges.front().second).type,
            EstimateType::MatchesApproximately);
  EXPECT_EQ(new_filter->estimate_cardinality(PredicateCondition::GreaterThan, ranges.front().second).type,
            EstimateType::MatchesNone);

  new_filter = std::static_pointer_cast<RangeFilter<TypeParam>>(
      filter->slice_with_predicate(PredicateCondition::GreaterThanEquals, 7));
  // New filter should start at 7 and end at same value as before.
  EXPECT_EQ(new_filter->estimate_cardinality(PredicateCondition::LessThan, 7).type, EstimateType::MatchesNone);
  EXPECT_EQ(new_filter->estimate_cardinality(PredicateCondition::LessThanEquals, 7).type,
            EstimateType::MatchesApproximately);
  EXPECT_EQ(new_filter->estimate_cardinality(PredicateCondition::GreaterThanEquals, ranges.back().second).type,
            EstimateType::MatchesApproximately);
  EXPECT_EQ(new_filter->estimate_cardinality(PredicateCondition::GreaterThan, ranges.back().second).type,
            EstimateType::MatchesNone);

  new_filter = std::static_pointer_cast<RangeFilter<TypeParam>>(
      filter->slice_with_predicate(PredicateCondition::GreaterThanEquals, 17));
  // New filter should start after first gap (because 17 is in that first gap) and end at same value as before.
  EXPECT_EQ(new_filter->estimate_cardinality(PredicateCondition::LessThan, ranges[1].first).type,
            EstimateType::MatchesNone);
  EXPECT_EQ(new_filter->estimate_cardinality(PredicateCondition::LessThanEquals, ranges[1].first).type,
            EstimateType::MatchesApproximately);
  EXPECT_EQ(new_filter->estimate_cardinality(PredicateCondition::GreaterThanEquals, ranges.back().second).type,
            EstimateType::MatchesApproximately);
  EXPECT_EQ(new_filter->estimate_cardinality(PredicateCondition::GreaterThan, ranges.back().second).type,
            EstimateType::MatchesNone);

  new_filter = std::static_pointer_cast<RangeFilter<TypeParam>>(
      filter->slice_with_predicate(PredicateCondition::Between, 7, 17));
  // New filter should start at 7 and end right before first gap (because 17 is in that gap).
  EXPECT_EQ(new_filter->estimate_cardinality(PredicateCondition::LessThan, 7).type, EstimateType::MatchesNone);
  EXPECT_EQ(new_filter->estimate_cardinality(PredicateCondition::LessThanEquals, 7).type,
            EstimateType::MatchesApproximately);
  EXPECT_EQ(new_filter->estimate_cardinality(PredicateCondition::GreaterThanEquals, ranges.front().second).type,
            EstimateType::MatchesApproximately);
  EXPECT_EQ(new_filter->estimate_cardinality(PredicateCondition::GreaterThan, ranges.front().second).type,
            EstimateType::MatchesNone);

  new_filter = std::static_pointer_cast<RangeFilter<TypeParam>>(
      filter->slice_with_predicate(PredicateCondition::Between, 17, 27));
  // New filter should start right after first gap (because 17 is in that gap)
  // and end right before second gap (because 27 is in that gap).
  EXPECT_EQ(new_filter->estimate_cardinality(PredicateCondition::LessThan, ranges[1].first).type,
            EstimateType::MatchesNone);
  EXPECT_EQ(new_filter->estimate_cardinality(PredicateCondition::LessThanEquals, ranges[1].first).type,
            EstimateType::MatchesApproximately);
  EXPECT_EQ(new_filter->estimate_cardinality(PredicateCondition::GreaterThanEquals, ranges[1].second).type,
            EstimateType::MatchesApproximately);
  EXPECT_EQ(new_filter->estimate_cardinality(PredicateCondition::GreaterThan, ranges[1].second).type,
            EstimateType::MatchesNone);

  // Slice with equality predicate will return MinMaxFilter.
  const auto min_max_filter =
      std::static_pointer_cast<MinMaxFilter<TypeParam>>(filter->slice_with_predicate(PredicateCondition::Equals, 7));
  // New filter should have 7 as min and max.
  EXPECT_EQ(min_max_filter->estimate_cardinality(PredicateCondition::LessThan, 7).type, EstimateType::MatchesNone);
  EXPECT_EQ(min_max_filter->estimate_cardinality(PredicateCondition::LessThanEquals, 7).type,
            EstimateType::MatchesApproximately);
  EXPECT_EQ(min_max_filter->estimate_cardinality(PredicateCondition::GreaterThanEquals, 7).type,
            EstimateType::MatchesApproximately);
  EXPECT_EQ(min_max_filter->estimate_cardinality(PredicateCondition::GreaterThan, 7).type, EstimateType::MatchesNone);
}

TYPED_TEST(RangeFilterTest, SliceWithPredicateEmptyStatistics) {
  const auto filter = RangeFilter<TypeParam>::build_filter(this->_values, 5);

  EXPECT_TRUE(std::dynamic_pointer_cast<EmptyStatisticsObject>(
      filter->slice_with_predicate(PredicateCondition::LessThan, this->_min_value)));
  EXPECT_FALSE(std::dynamic_pointer_cast<EmptyStatisticsObject>(
      filter->slice_with_predicate(PredicateCondition::LessThanEquals, this->_min_value)));
  EXPECT_FALSE(std::dynamic_pointer_cast<EmptyStatisticsObject>(
      filter->slice_with_predicate(PredicateCondition::GreaterThanEquals, this->_max_value)));
  EXPECT_TRUE(std::dynamic_pointer_cast<EmptyStatisticsObject>(
      filter->slice_with_predicate(PredicateCondition::GreaterThan, this->_max_value)));
}

}  // namespace opossum
