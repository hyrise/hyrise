#include <functional>
#include <memory>
#include <random>
#include <string>
#include <utility>
#include <vector>

#include "base_test.hpp"
#include "gtest/gtest.h"

#include "utils/assert.hpp"

#include "statistics/chunk_statistics/range_filter.hpp"
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
      EXPECT_FALSE(filter->can_prune(PredicateCondition::Equals, {value}));
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
      EXPECT_FALSE(filter->can_prune(PredicateCondition::Equals, {begin}));
      EXPECT_FALSE(filter->can_prune(PredicateCondition::Equals, {end}));
      if constexpr (std::numeric_limits<T>::is_iec559) {
        auto value_in_gap = begin + 0.5 * length;
        EXPECT_TRUE(filter->can_prune(PredicateCondition::Equals, {value_in_gap}));
      } else if constexpr (std::is_integral_v<T>) {  // NOLINT
        if (length > 1) {
          EXPECT_TRUE(filter->can_prune(PredicateCondition::Equals, {begin + 1}));
          EXPECT_TRUE(filter->can_prune(PredicateCondition::Equals, {end - 1}));
        }
      }
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
  EXPECT_FALSE(filter->can_prune(PredicateCondition::Equals, 0));
  // Nonetheless, the filter should prune values outside the single range.
  EXPECT_TRUE(filter->can_prune(PredicateCondition::Equals, std::numeric_limits<TypeParam>::lowest() * 0.95));
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
    EXPECT_FALSE(filter->can_prune(PredicateCondition::Equals, {value}));
  }

  // testing for interval bounds
  EXPECT_TRUE(filter->can_prune(PredicateCondition::LessThan, {this->_min_value}));
  EXPECT_FALSE(filter->can_prune(PredicateCondition::GreaterThan, {this->_min_value}));

  // cannot prune values in between, even though non-existent
  EXPECT_FALSE(filter->can_prune(PredicateCondition::Equals, {this->_in_between}));

  EXPECT_FALSE(filter->can_prune(PredicateCondition::LessThanEquals, {this->_max_value}));
  EXPECT_TRUE(filter->can_prune(PredicateCondition::GreaterThan, {this->_max_value}));
}

// create range filters with varying number of ranges/gaps
TYPED_TEST(RangeFilterTest, MultipleRanges) {
  for (auto gap_count = size_t{0}; gap_count < this->_values.size() * 2; ++gap_count) {
    const auto filter = this->test_varying_range_filter_size(gap_count, this->_values);

    // _in_between should always prune if we have more than one range
    if (gap_count > 1) {
      EXPECT_TRUE(filter->can_prune(PredicateCondition::Equals, this->_in_between));
    }
  }
}

// create more ranges than distinct values in the test data
TYPED_TEST(RangeFilterTest, MoreRangesThanValues) {
  const auto filter = RangeFilter<TypeParam>::build_filter(this->_values, 10'000);

  for (const auto& value : this->_values) {
    EXPECT_FALSE(filter->can_prune(PredicateCondition::Equals, {value}));
  }

  // testing for interval bounds
  EXPECT_TRUE(filter->can_prune(PredicateCondition::LessThan, {this->_min_value}));
  EXPECT_FALSE(filter->can_prune(PredicateCondition::GreaterThan, {this->_min_value}));
  EXPECT_TRUE(filter->can_prune(PredicateCondition::Equals, {this->_in_between}));
  EXPECT_FALSE(filter->can_prune(PredicateCondition::LessThanEquals, {this->_max_value}));
  EXPECT_TRUE(filter->can_prune(PredicateCondition::GreaterThan, {this->_max_value}));
}

// this test checks the correct pruning on the bounds (min/max) of the test data for various predicate conditions
// for better understanding, see min_max_filter_test.cpp
TYPED_TEST(RangeFilterTest, CanPruneOnBounds) {
  const auto filter = RangeFilter<TypeParam>::build_filter(this->_values);

  for (const auto& value : this->_values) {
    EXPECT_FALSE(filter->can_prune(PredicateCondition::Equals, {value}));
  }

  EXPECT_TRUE(filter->can_prune(PredicateCondition::LessThan, {this->_before_range}));
  EXPECT_TRUE(filter->can_prune(PredicateCondition::LessThan, {this->_min_value}));
  EXPECT_FALSE(filter->can_prune(PredicateCondition::LessThan, {this->_in_between}));
  EXPECT_FALSE(filter->can_prune(PredicateCondition::LessThan, {this->_max_value}));
  EXPECT_FALSE(filter->can_prune(PredicateCondition::LessThan, {this->_after_range}));

  EXPECT_TRUE(filter->can_prune(PredicateCondition::LessThanEquals, {this->_before_range}));
  EXPECT_FALSE(filter->can_prune(PredicateCondition::LessThanEquals, {this->_min_value}));
  EXPECT_FALSE(filter->can_prune(PredicateCondition::LessThanEquals, {this->_in_between}));
  EXPECT_FALSE(filter->can_prune(PredicateCondition::LessThanEquals, {this->_max_value}));
  EXPECT_FALSE(filter->can_prune(PredicateCondition::LessThanEquals, {this->_after_range}));

  EXPECT_TRUE(filter->can_prune(PredicateCondition::Equals, {this->_before_range}));
  EXPECT_FALSE(filter->can_prune(PredicateCondition::Equals, {this->_min_value}));
  EXPECT_TRUE(filter->can_prune(PredicateCondition::Equals, {this->_in_between}));
  EXPECT_FALSE(filter->can_prune(PredicateCondition::Equals, {this->_max_value}));
  EXPECT_TRUE(filter->can_prune(PredicateCondition::Equals, {this->_after_range}));

  EXPECT_FALSE(filter->can_prune(PredicateCondition::GreaterThanEquals, {this->_before_range}));
  EXPECT_FALSE(filter->can_prune(PredicateCondition::GreaterThanEquals, {this->_min_value}));
  EXPECT_FALSE(filter->can_prune(PredicateCondition::GreaterThanEquals, {this->_in_between}));
  EXPECT_FALSE(filter->can_prune(PredicateCondition::GreaterThanEquals, {this->_max_value}));
  EXPECT_TRUE(filter->can_prune(PredicateCondition::GreaterThanEquals, {this->_after_range}));

  EXPECT_FALSE(filter->can_prune(PredicateCondition::GreaterThan, {this->_before_range}));
  EXPECT_FALSE(filter->can_prune(PredicateCondition::GreaterThan, {this->_min_value}));
  EXPECT_FALSE(filter->can_prune(PredicateCondition::GreaterThan, {this->_in_between}));
  EXPECT_TRUE(filter->can_prune(PredicateCondition::GreaterThan, {this->_max_value}));
  EXPECT_TRUE(filter->can_prune(PredicateCondition::GreaterThan, {this->_after_range}));
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
      EXPECT_TRUE(
          filter->can_prune(PredicateCondition::Equals, {get_random_number<TypeParam>(rng, middle_gap_distribution)}));
    }
  }
}

}  // namespace opossum
