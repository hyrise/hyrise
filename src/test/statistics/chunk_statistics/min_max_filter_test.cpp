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
class MinMaxFilterTest<pmr_string> : public ::testing::Test {
 protected:
  void SetUp() override {
    _values = pmr_vector<pmr_string>{"aa", "bb", "b", "bbbbba", "bbbbbb", "bbbbbc", "c"};
    _min_value = *std::min_element(std::begin(_values), std::end(_values));
    _max_value = *std::max_element(std::begin(_values), std::end(_values));
    _in_between = "ba";   // value in between the min and max
    _before_range = "a";  // value smaller/before than the minimum
    _after_range = "cc";  // value larger/beyond than the maximum
  }

  pmr_vector<pmr_string> _values;
  pmr_string _before_range, _min_value, _max_value, _after_range, _in_between;
};

using FilterTypes = ::testing::Types<int, float, double, pmr_string>;
TYPED_TEST_CASE(MinMaxFilterTest, FilterTypes, );  // NOLINT(whitespace/parens)

TYPED_TEST(MinMaxFilterTest, CanPruneOnBounds) {
  auto filter = std::make_unique<MinMaxFilter<TypeParam>>(this->_values.front(), this->_values.back());

  for (const auto& value : this->_values) {
    EXPECT_FALSE(filter->can_prune(PredicateCondition::Equals, {value}));
  }

  // for the predicate condition of <, we expect only values smaller or equal to the minimum to be prunable
  EXPECT_TRUE(filter->can_prune(PredicateCondition::LessThan, {this->_before_range}));
  EXPECT_TRUE(filter->can_prune(PredicateCondition::LessThan, {this->_min_value}));
  EXPECT_FALSE(filter->can_prune(PredicateCondition::LessThan, {this->_in_between}));
  EXPECT_FALSE(filter->can_prune(PredicateCondition::LessThan, {this->_max_value}));
  EXPECT_FALSE(filter->can_prune(PredicateCondition::LessThan, {this->_after_range}));

  // for the predicate condition of <=, we expect only values smaller than the minimum to be prunable
  EXPECT_TRUE(filter->can_prune(PredicateCondition::LessThanEquals, {this->_before_range}));
  EXPECT_FALSE(filter->can_prune(PredicateCondition::LessThanEquals, {this->_min_value}));
  EXPECT_FALSE(filter->can_prune(PredicateCondition::LessThanEquals, {this->_in_between}));
  EXPECT_FALSE(filter->can_prune(PredicateCondition::LessThanEquals, {this->_max_value}));
  EXPECT_FALSE(filter->can_prune(PredicateCondition::LessThanEquals, {this->_after_range}));

  // for the predicate condition of ==, we expect only values outside the max/max range to be prunable
  EXPECT_TRUE(filter->can_prune(PredicateCondition::Equals, {this->_before_range}));
  EXPECT_FALSE(filter->can_prune(PredicateCondition::Equals, {this->_min_value}));
  EXPECT_FALSE(filter->can_prune(PredicateCondition::Equals, {this->_in_between}));
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

  // as null values are not comparable, we never prune them
  EXPECT_FALSE(filter->can_prune(PredicateCondition::IsNull, NULL_VALUE));
  EXPECT_FALSE(filter->can_prune(PredicateCondition::IsNull, {this->_in_between}));
  EXPECT_FALSE(filter->can_prune(PredicateCondition::IsNull, {this->_min_value}, {this->_in_between}));
  EXPECT_FALSE(filter->can_prune(PredicateCondition::IsNotNull, NULL_VALUE));
  EXPECT_FALSE(filter->can_prune(PredicateCondition::IsNotNull, {this->_in_between}));
  EXPECT_FALSE(filter->can_prune(PredicateCondition::IsNotNull, {this->_min_value}, {this->_in_between}));
}

}  // namespace opossum
