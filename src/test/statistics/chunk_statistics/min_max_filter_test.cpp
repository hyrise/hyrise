#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "base_test.hpp"
#include "gtest/gtest.h"

#include "utils/assert.hpp"

#include "statistics/chunk_statistics/min_max_filter.hpp"
#include "types.hpp"

namespace opossum {

template <typename T>
class MinMaxFilterTest : public ::testing::Test {
 protected:
  void SetUp() override {
    _values = pmr_vector<T>{-1000, 2, 3, 4, 7, 8, 10, 17, 123456};
    _min_value = *std::min_element(std::begin(_values), std::end(_values));
    _max_value = *std::max_element(std::begin(_values), std::end(_values));
    _in_between = static_cast<T>(_min_value + 0.5 * (_max_value - _min_value));  // value in between the min and max
    _before_range = _min_value - 1;                                              // value smaller than the minimum
    _after_range = _max_value + 1;                                               // value larger than the maximum
  }

  pmr_vector<T> _values;
  T _before_range, _min_value, _max_value, _after_range, _in_between;
};

// the test data for strings needs to be handled differently from numerics
template <>
class MinMaxFilterTest<std::string> : public ::testing::Test {
 protected:
  void SetUp() override {
    _values = pmr_vector<std::string>{"aa", "bb", "b", "bbbbba", "bbbbbb", "bbbbbc", "c"};
    _min_value = *std::min_element(std::begin(_values), std::end(_values));
    _max_value = *std::max_element(std::begin(_values), std::end(_values));
    _in_between = "ba";   // value in between the min and max
    _before_range = "a";  // value smaller/before than the minimum
    _after_range = "cc";  // value larger/beyond than the maximum
  }

  pmr_vector<std::string> _values;
  std::string _before_range, _min_value, _max_value, _after_range, _in_between;
};

using FilterTypes = ::testing::Types<int, float, double, std::string>;
TYPED_TEST_CASE(MinMaxFilterTest, FilterTypes);

TYPED_TEST(MinMaxFilterTest, CanPruneOnBounds) {
  auto filter = std::make_unique<MinMaxFilter<TypeParam>>(this->_values.front(), this->_values.back());

  for (const auto& value : this->_values) {
    EXPECT_FALSE(filter->can_prune({value}, PredicateCondition::Equals));
  }

  // for the predicate condition of <, we expect only values smaller or equal to the minimum to be prunable
  EXPECT_TRUE(filter->can_prune({this->_before_range}, PredicateCondition::LessThan));
  EXPECT_TRUE(filter->can_prune({this->_min_value}, PredicateCondition::LessThan));
  EXPECT_FALSE(filter->can_prune({this->_in_between}, PredicateCondition::LessThan));
  EXPECT_FALSE(filter->can_prune({this->_max_value}, PredicateCondition::LessThan));
  EXPECT_FALSE(filter->can_prune({this->_after_range}, PredicateCondition::LessThan));

  // for the predicate condition of <=, we expect only values smaller than the minimum to be prunable
  EXPECT_TRUE(filter->can_prune({this->_before_range}, PredicateCondition::LessThanEquals));
  EXPECT_FALSE(filter->can_prune({this->_min_value}, PredicateCondition::LessThanEquals));
  EXPECT_FALSE(filter->can_prune({this->_in_between}, PredicateCondition::LessThanEquals));
  EXPECT_FALSE(filter->can_prune({this->_max_value}, PredicateCondition::LessThanEquals));
  EXPECT_FALSE(filter->can_prune({this->_after_range}, PredicateCondition::LessThanEquals));

  // for the predicate condition of ==, we expect only values outside the max/max range to be prunable
  EXPECT_TRUE(filter->can_prune({this->_before_range}, PredicateCondition::Equals));
  EXPECT_FALSE(filter->can_prune({this->_min_value}, PredicateCondition::Equals));
  EXPECT_FALSE(filter->can_prune({this->_in_between}, PredicateCondition::Equals));
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

  // as null values are not comparable, we never prune them
  EXPECT_FALSE(filter->can_prune({this->_in_between}, PredicateCondition::IsNull));
}

}  // namespace opossum
