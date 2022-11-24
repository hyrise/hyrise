#include <algorithm>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "base_test.hpp"
#include "utils/assert.hpp"

#include "statistics/statistics_objects/min_max_filter.hpp"
#include "types.hpp"

namespace hyrise {

template <typename T>
class MinMaxFilterTest : public BaseTest {
 protected:
  void SetUp() override {
    _values = pmr_vector<T>{-1000, 2, 3, 4, 7, 8, 10, 17, 123456};
    _min_value = *std::min_element(std::begin(_values), std::end(_values));
    _max_value = *std::max_element(std::begin(_values), std::end(_values));
    _in_between = static_cast<T>(_min_value + 0.5 * (_max_value - _min_value));  // value in between the min and max
    _in_between2 =
        static_cast<T>(_in_between + 0.5 * (_max_value - _in_between));  // value in between _in_between and max
    _before_range = _min_value - 1;                                      // value smaller than the minimum
    _after_range = _max_value + 1;                                       // value larger than the maximum
  }

  pmr_vector<T> _values;
  T _before_range, _min_value, _max_value, _after_range, _in_between, _in_between2;
};

// the test data for strings needs to be handled differently from numerics
template <>
class MinMaxFilterTest<pmr_string> : public BaseTest {
 protected:
  void SetUp() override {
    _values = pmr_vector<pmr_string>{"aa", "bb", "b", "bbbbba", "bbbbbb", "bbbbbc", "c"};
    _min_value = *std::min_element(std::begin(_values), std::end(_values));
    _max_value = *std::max_element(std::begin(_values), std::end(_values));
    _in_between = "ba";   // value in between the min and max
    _in_between2 = "bm";  // value in between _in_between and max
    _before_range = "a";  // value smaller/before than the minimum
    _after_range = "cc";  // value larger/beyond than the maximum
  }

  pmr_vector<pmr_string> _values;
  pmr_string _before_range, _min_value, _max_value, _after_range, _in_between, _in_between2;
};

class MinMaxFilterTestLike : public BaseTest {};

TEST_F(MinMaxFilterTestLike, CanPruneLike) {
  auto max_ascii_value = pmr_string(1, static_cast<char>(127));
  max_ascii_value.append("%");

  const auto filter = std::make_unique<MinMaxFilter<pmr_string>>("b", "c");
  // For the predicate condition of Like, we expect only values where the lower_bound is bigger than the max value or
  // the upper_bound is smaller or equal to the min value to be prunable. In the following tests `b` would be the min
  // value and `c` the max.
  EXPECT_TRUE(filter->does_not_contain(PredicateCondition::Like, "aa%"));
  EXPECT_TRUE(filter->does_not_contain(PredicateCondition::Like, "aa_"));
  EXPECT_TRUE(filter->does_not_contain(PredicateCondition::Like, "cc%"));
  EXPECT_TRUE(filter->does_not_contain(PredicateCondition::Like, "cc_"));
  EXPECT_TRUE(filter->does_not_contain(PredicateCondition::Like, "a"));
  EXPECT_TRUE(filter->does_not_contain(PredicateCondition::Like, "a%"));

  EXPECT_FALSE(filter->does_not_contain(PredicateCondition::Like, max_ascii_value));
  EXPECT_FALSE(filter->does_not_contain(PredicateCondition::Like, "b%"));
  EXPECT_FALSE(filter->does_not_contain(PredicateCondition::Like, "bbbb%"));
  EXPECT_FALSE(filter->does_not_contain(PredicateCondition::Like, "c%"));
  EXPECT_FALSE(filter->does_not_contain(PredicateCondition::Like, "%"));
  EXPECT_FALSE(filter->does_not_contain(PredicateCondition::Like, "_"));
  EXPECT_FALSE(filter->does_not_contain(PredicateCondition::Like, "_%"));
  EXPECT_FALSE(filter->does_not_contain(PredicateCondition::Like, "b"));
  EXPECT_FALSE(filter->does_not_contain(PredicateCondition::Like, "c"));

  // For the predicate condition of NotLike, we expect only values where the lower_bound is smaller or equal than the
  // min value and the upper_bound is bigger to the max value to be prunable. In the following tests `b` would be the
  // min value and `c` the max.
  EXPECT_FALSE(filter->does_not_contain(PredicateCondition::NotLike, "b%"));
  EXPECT_FALSE(filter->does_not_contain(PredicateCondition::NotLike, "b"));
  EXPECT_FALSE(filter->does_not_contain(PredicateCondition::NotLike, "%"));
  EXPECT_FALSE(filter->does_not_contain(PredicateCondition::NotLike, "a%"));
  EXPECT_FALSE(filter->does_not_contain(PredicateCondition::NotLike, "c%"));
  EXPECT_FALSE(filter->does_not_contain(PredicateCondition::NotLike, "bb%"));
  EXPECT_FALSE(filter->does_not_contain(PredicateCondition::NotLike, "d%"));
  EXPECT_FALSE(filter->does_not_contain(PredicateCondition::NotLike, "aa%"));

  const auto filter_equal_values = std::make_unique<MinMaxFilter<pmr_string>>("a", "a");
  EXPECT_TRUE(filter_equal_values->does_not_contain(PredicateCondition::NotLike, "a"));

  const auto filter_max_ascii = std::make_unique<MinMaxFilter<pmr_string>>(pmr_string(1, static_cast<char>(127)),
                                                                           pmr_string(1, static_cast<char>(127)));
  // The following test should make sure, that if the last character of the upper bound would be an ASCII overflow,
  // `does_not_contain` returns false.
  EXPECT_FALSE(filter_max_ascii->does_not_contain(PredicateCondition::NotLike, max_ascii_value));
  EXPECT_FALSE(filter_max_ascii->does_not_contain(PredicateCondition::Like, max_ascii_value));
  EXPECT_FALSE(filter_max_ascii->does_not_contain(PredicateCondition::NotLike, max_ascii_value));

  // We use the ascii collation for min/max filters. This means that lower case letters are considered larger than
  // upper case letters. In this test USA% is not pruned since t is a lower case value.
  const auto filter_max_lower_case = std::make_unique<MinMaxFilter<pmr_string>>("T", "t");
  EXPECT_FALSE(filter_max_lower_case->does_not_contain(PredicateCondition::Like, "USA%"));
}

using MixMaxFilterTypes = ::testing::Types<int, float, double, pmr_string>;
TYPED_TEST_SUITE(MinMaxFilterTest, MixMaxFilterTypes, );  // NOLINT(whitespace/parens)

TYPED_TEST(MinMaxFilterTest, CanPruneOnBounds) {
  auto filter = std::make_unique<MinMaxFilter<TypeParam>>(this->_values.front(), this->_values.back());

  for (const auto& value : this->_values) {
    EXPECT_FALSE(filter->does_not_contain(PredicateCondition::Equals, {value}));
  }

  // for the predicate condition of <, we expect only values smaller or equal to the minimum to be prunable
  EXPECT_TRUE(filter->does_not_contain(PredicateCondition::LessThan, {this->_before_range}));
  EXPECT_TRUE(filter->does_not_contain(PredicateCondition::LessThan, {this->_min_value}));
  EXPECT_FALSE(filter->does_not_contain(PredicateCondition::LessThan, {this->_in_between}));
  EXPECT_FALSE(filter->does_not_contain(PredicateCondition::LessThan, {this->_max_value}));
  EXPECT_FALSE(filter->does_not_contain(PredicateCondition::LessThan, {this->_after_range}));

  // for the predicate condition of <=, we expect only values smaller than the minimum to be prunable
  EXPECT_TRUE(filter->does_not_contain(PredicateCondition::LessThanEquals, {this->_before_range}));
  EXPECT_FALSE(filter->does_not_contain(PredicateCondition::LessThanEquals, {this->_min_value}));
  EXPECT_FALSE(filter->does_not_contain(PredicateCondition::LessThanEquals, {this->_in_between}));
  EXPECT_FALSE(filter->does_not_contain(PredicateCondition::LessThanEquals, {this->_max_value}));
  EXPECT_FALSE(filter->does_not_contain(PredicateCondition::LessThanEquals, {this->_after_range}));

  // for the predicate condition of ==, we expect only values outside the max/max range to be prunable
  EXPECT_TRUE(filter->does_not_contain(PredicateCondition::Equals, {this->_before_range}));
  EXPECT_FALSE(filter->does_not_contain(PredicateCondition::Equals, {this->_min_value}));
  EXPECT_FALSE(filter->does_not_contain(PredicateCondition::Equals, {this->_in_between}));
  EXPECT_FALSE(filter->does_not_contain(PredicateCondition::Equals, {this->_max_value}));
  EXPECT_TRUE(filter->does_not_contain(PredicateCondition::Equals, {this->_after_range}));

  EXPECT_FALSE(filter->does_not_contain(PredicateCondition::GreaterThanEquals, {this->_before_range}));
  EXPECT_FALSE(filter->does_not_contain(PredicateCondition::GreaterThanEquals, {this->_min_value}));
  EXPECT_FALSE(filter->does_not_contain(PredicateCondition::GreaterThanEquals, {this->_in_between}));
  EXPECT_FALSE(filter->does_not_contain(PredicateCondition::GreaterThanEquals, {this->_max_value}));
  EXPECT_TRUE(filter->does_not_contain(PredicateCondition::GreaterThanEquals, {this->_after_range}));

  EXPECT_FALSE(filter->does_not_contain(PredicateCondition::GreaterThan, {this->_before_range}));
  EXPECT_FALSE(filter->does_not_contain(PredicateCondition::GreaterThan, {this->_min_value}));
  EXPECT_FALSE(filter->does_not_contain(PredicateCondition::GreaterThan, {this->_in_between}));
  EXPECT_TRUE(filter->does_not_contain(PredicateCondition::GreaterThan, {this->_max_value}));
  EXPECT_TRUE(filter->does_not_contain(PredicateCondition::GreaterThan, {this->_after_range}));

  EXPECT_FALSE(
      filter->does_not_contain(PredicateCondition::BetweenInclusive, {this->_before_range}, {this->_min_value}));
  EXPECT_FALSE(filter->does_not_contain(PredicateCondition::BetweenInclusive, {this->_min_value}, {this->_in_between}));
  EXPECT_FALSE(
      filter->does_not_contain(PredicateCondition::BetweenInclusive, {this->_in_between}, {this->_in_between2}));
  EXPECT_FALSE(
      filter->does_not_contain(PredicateCondition::BetweenInclusive, {this->_in_between2}, {this->_max_value}));
  EXPECT_FALSE(
      filter->does_not_contain(PredicateCondition::BetweenInclusive, {this->_max_value}, {this->_after_range}));

  EXPECT_FALSE(
      filter->does_not_contain(PredicateCondition::BetweenLowerExclusive, {this->_before_range}, {this->_min_value}));
  EXPECT_FALSE(
      filter->does_not_contain(PredicateCondition::BetweenLowerExclusive, {this->_min_value}, {this->_in_between}));
  EXPECT_FALSE(
      filter->does_not_contain(PredicateCondition::BetweenLowerExclusive, {this->_in_between}, {this->_in_between2}));
  EXPECT_FALSE(
      filter->does_not_contain(PredicateCondition::BetweenLowerExclusive, {this->_in_between2}, {this->_max_value}));
  EXPECT_TRUE(
      filter->does_not_contain(PredicateCondition::BetweenLowerExclusive, {this->_max_value}, {this->_after_range}));

  EXPECT_TRUE(
      filter->does_not_contain(PredicateCondition::BetweenUpperExclusive, {this->_before_range}, {this->_min_value}));
  EXPECT_FALSE(
      filter->does_not_contain(PredicateCondition::BetweenUpperExclusive, {this->_min_value}, {this->_in_between}));
  EXPECT_FALSE(
      filter->does_not_contain(PredicateCondition::BetweenUpperExclusive, {this->_in_between}, {this->_in_between2}));
  EXPECT_FALSE(
      filter->does_not_contain(PredicateCondition::BetweenUpperExclusive, {this->_in_between2}, {this->_max_value}));
  EXPECT_FALSE(
      filter->does_not_contain(PredicateCondition::BetweenUpperExclusive, {this->_max_value}, {this->_after_range}));

  EXPECT_TRUE(
      filter->does_not_contain(PredicateCondition::BetweenExclusive, {this->_before_range}, {this->_min_value}));
  EXPECT_FALSE(filter->does_not_contain(PredicateCondition::BetweenExclusive, {this->_min_value}, {this->_in_between}));
  EXPECT_FALSE(
      filter->does_not_contain(PredicateCondition::BetweenExclusive, {this->_in_between}, {this->_in_between2}));
  EXPECT_FALSE(
      filter->does_not_contain(PredicateCondition::BetweenExclusive, {this->_in_between2}, {this->_max_value}));
  EXPECT_TRUE(filter->does_not_contain(PredicateCondition::BetweenExclusive, {this->_max_value}, {this->_after_range}));

  EXPECT_FALSE(filter->does_not_contain(PredicateCondition::IsNull, NULL_VALUE));
  EXPECT_FALSE(filter->does_not_contain(PredicateCondition::IsNull, {this->_in_between}));
  EXPECT_FALSE(filter->does_not_contain(PredicateCondition::IsNull, {this->_min_value}, {this->_in_between}));
  EXPECT_FALSE(filter->does_not_contain(PredicateCondition::IsNotNull, NULL_VALUE));
  EXPECT_FALSE(filter->does_not_contain(PredicateCondition::IsNotNull, {this->_in_between}));
  EXPECT_FALSE(filter->does_not_contain(PredicateCondition::IsNotNull, {this->_min_value}, {this->_in_between}));

  // as null values are not comparable, we never prune them
  EXPECT_FALSE(filter->does_not_contain(PredicateCondition::IsNull, {this->_in_between}));
}

TYPED_TEST(MinMaxFilterTest, Sliced) {
  auto new_filter = std::shared_ptr<MinMaxFilter<TypeParam>>{};

  const auto filter = std::make_unique<MinMaxFilter<TypeParam>>(this->_values.front(), this->_values.back());

  new_filter =
      std::static_pointer_cast<MinMaxFilter<TypeParam>>(filter->sliced(PredicateCondition::Equals, this->_in_between));
  EXPECT_EQ(new_filter->min, this->_in_between);
  EXPECT_EQ(new_filter->max, this->_in_between);

  new_filter = std::static_pointer_cast<MinMaxFilter<TypeParam>>(
      filter->sliced(PredicateCondition::NotEquals, this->_in_between));
  EXPECT_EQ(new_filter->min, this->_min_value);
  EXPECT_EQ(new_filter->max, this->_max_value);

  new_filter = std::static_pointer_cast<MinMaxFilter<TypeParam>>(
      filter->sliced(PredicateCondition::LessThanEquals, this->_in_between));
  EXPECT_EQ(new_filter->min, this->_min_value);
  EXPECT_EQ(new_filter->max, this->_in_between);

  new_filter = std::static_pointer_cast<MinMaxFilter<TypeParam>>(
      filter->sliced(PredicateCondition::GreaterThanEquals, this->_in_between));
  EXPECT_EQ(new_filter->min, this->_in_between);
  EXPECT_EQ(new_filter->max, this->_max_value);

  new_filter = std::static_pointer_cast<MinMaxFilter<TypeParam>>(
      filter->sliced(PredicateCondition::BetweenInclusive, this->_in_between, this->_in_between2));
  EXPECT_EQ(new_filter->min, this->_in_between);
  EXPECT_EQ(new_filter->max, this->_in_between2);
}

TYPED_TEST(MinMaxFilterTest, SliceWithPredicateReturnsNullptr) {
  const auto filter = std::make_unique<MinMaxFilter<TypeParam>>(this->_values.front(), this->_values.back());

  EXPECT_EQ(filter->sliced(PredicateCondition::LessThan, this->_values.front()), nullptr);
  EXPECT_NE(filter->sliced(PredicateCondition::LessThanEquals, this->_values.front()), nullptr);
  EXPECT_NE(filter->sliced(PredicateCondition::GreaterThanEquals, this->_values.back()), nullptr);
  EXPECT_EQ(filter->sliced(PredicateCondition::GreaterThan, this->_values.back()), nullptr);
}

}  // namespace hyrise
