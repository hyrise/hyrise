#include <memory>
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
    _in_between = static_cast<T>(_min_value + 0.5 * (_max_value - _min_value));
    _before_range = _min_value - 1;
    _after_range = _max_value + 1;
  }

  void test_varying_range_filter_size(int gap_count) {
    // RangeFilter constructor takes range count, not gap count 
    auto filter = RangeFilter<T>::build_filter(_values, gap_count + 1);

    for (const auto& value : _values) {
      EXPECT_FALSE(filter->can_prune({value}, PredicateCondition::Equals));
    }

    // find $gap_count largest gaps
    auto value_set = std::set<T>(_values.begin(), _values.end());
    std::vector<std::pair<T, T>> begin_length_pairs;

    for (auto it = value_set.begin(); it != std::prev(value_set.end()); ++it) {
      auto begin = *it;
      auto end = *(std::next(it));
      begin_length_pairs.push_back(std::make_pair(begin, end-begin));
    }

    std::sort(begin_length_pairs.begin(), begin_length_pairs.end(), [](auto &left, auto &right) {
      return left.second > right.second;
    });

    for (int i = 0; i < gap_count && i < static_cast<int>(begin_length_pairs.size()); ++i) {
      auto gap = begin_length_pairs[i];
      auto begin = gap.first;
      auto length = gap.second;
      auto end = begin + length; 
      EXPECT_FALSE(filter->can_prune({begin}, PredicateCondition::Equals));
      EXPECT_FALSE(filter->can_prune({end}, PredicateCondition::Equals));
      if (std::numeric_limits<T>::is_iec559) {
        auto value_in_gap = begin + 0.5 * length;
        EXPECT_TRUE(filter->can_prune({value_in_gap}, PredicateCondition::Equals));
      } else if (std::is_integral<T>::value && length > 1) {
        EXPECT_TRUE(filter->can_prune({++begin}, PredicateCondition::Equals));
      }
    }
  }

  pmr_vector<T> _values;
  T _before_range, _min_value, _max_value, _after_range, _in_between;
};

using FilterTypes = ::testing::Types<int, float, double>;
TYPED_TEST_CASE(RangeFilterTest, FilterTypes);

TYPED_TEST(RangeFilterTest, SingleRange) {
  auto filter = RangeFilter<TypeParam>::build_filter(this->_values, 1);

  for (const auto& value : this->_values) {
    EXPECT_FALSE(filter->can_prune({value}, PredicateCondition::Equals));
  }

  // testing for interval bounds
  EXPECT_TRUE(filter->can_prune({this->_min_value}, PredicateCondition::LessThan));
  EXPECT_FALSE(filter->can_prune({this->_min_value}, PredicateCondition::GreaterThan));
  EXPECT_FALSE(filter->can_prune({this->_max_value}, PredicateCondition::LessThanEquals));
  EXPECT_TRUE(filter->can_prune({this->_max_value}, PredicateCondition::GreaterThan));

  // cannot prune values in between, even though non-existent
  EXPECT_FALSE(filter->can_prune({static_cast<TypeParam>(20)}, PredicateCondition::Equals));
}

TYPED_TEST(RangeFilterTest, MultipleRanges) {
  for (unsigned long i = 0; i < this->_values.size() * 2; ++i) {
    this->test_varying_range_filter_size(static_cast<int>(i));
  }
}

TYPED_TEST(RangeFilterTest, MoreRangesThanValues) {
  auto filter = RangeFilter<TypeParam>::build_filter(this->_values, 10'000);

  for (const auto& value : this->_values) {
    EXPECT_FALSE(filter->can_prune({value}, PredicateCondition::Equals));
  }

  // testing for interval bounds
  EXPECT_TRUE(filter->can_prune({this->_min_value}, PredicateCondition::LessThan));
  EXPECT_FALSE(filter->can_prune({this->_min_value}, PredicateCondition::GreaterThan));
  EXPECT_FALSE(filter->can_prune({this->_max_value}, PredicateCondition::LessThanEquals));
  EXPECT_TRUE(filter->can_prune({this->_max_value}, PredicateCondition::GreaterThan));
}

TYPED_TEST(RangeFilterTest, CanPruneOnBounds) {
  auto filter = RangeFilter<TypeParam>::build_filter(this->_values);

  for (const auto& value : this->_values) {
    EXPECT_FALSE(filter->can_prune({value}, PredicateCondition::Equals));
  }

  EXPECT_TRUE(filter->can_prune({this->_before_range}, PredicateCondition::LessThan));
  EXPECT_TRUE(filter->can_prune({this->_min_value}, PredicateCondition::LessThan));
  EXPECT_FALSE(filter->can_prune({this->_in_between}, PredicateCondition::LessThan));
  EXPECT_FALSE(filter->can_prune({this->_max_value}, PredicateCondition::LessThan));
  EXPECT_FALSE(filter->can_prune({this->_after_range}, PredicateCondition::LessThan));

  EXPECT_TRUE(filter->can_prune({this->_before_range}, PredicateCondition::LessThanEquals));
  EXPECT_FALSE(filter->can_prune({this->_min_value}, PredicateCondition::LessThanEquals));
  EXPECT_FALSE(filter->can_prune({this->_in_between}, PredicateCondition::LessThanEquals));
  EXPECT_FALSE(filter->can_prune({this->_max_value}, PredicateCondition::LessThanEquals));
  EXPECT_FALSE(filter->can_prune({this->_after_range}, PredicateCondition::LessThanEquals));

  // cannot check for _in_between since calculated to in range but does not need to exist
  EXPECT_TRUE(filter->can_prune({this->_before_range}, PredicateCondition::Equals));
  EXPECT_FALSE(filter->can_prune({this->_min_value}, PredicateCondition::Equals));
  EXPECT_FALSE(filter->can_prune({this->_max_value}, PredicateCondition::Equals));
  EXPECT_TRUE(filter->can_prune({this->_after_range}, PredicateCondition::Equals));

  EXPECT_FALSE(filter->can_prune({this->_before_range}, PredicateCondition::GreaterThanEquals));
  EXPECT_FALSE(filter->can_prune({this->_min_value}, PredicateCondition::GreaterThanEquals));
  EXPECT_FALSE(filter->can_prune({this->_in_between}, PredicateCondition::GreaterThanEquals));
  EXPECT_FALSE(filter->can_prune({this->_max_value}, PredicateCondition::GreaterThanEquals));
  EXPECT_TRUE(filter->can_prune({this->_after_range}, PredicateCondition::GreaterThanEquals));

  EXPECT_FALSE(filter->can_prune({this->_before_range}, PredicateCondition::GreaterThan));
  EXPECT_FALSE(filter->can_prune({this->_min_value}, PredicateCondition::GreaterThan));
  EXPECT_FALSE(filter->can_prune({this->_in_between}, PredicateCondition::GreaterThan));
  EXPECT_TRUE(filter->can_prune({this->_max_value}, PredicateCondition::GreaterThan));
  EXPECT_TRUE(filter->can_prune({this->_after_range}, PredicateCondition::GreaterThan));
}

}  // namespace opossum
