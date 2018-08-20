#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "base_test.hpp"
#include "gtest/gtest.h"

#include "utils/assert.hpp"

#include "statistics/chunk_statistics/min_max_filter.hpp"
#include "statistics/chunk_statistics/range_filter.hpp"
#include "types.hpp"

namespace opossum {

class PruningFiltersTest : public BaseTest {
 protected:
  void SetUp() override {
    _values = pmr_vector<int>{-1000, 2, 3, 4, 7, 8, 10, 123456};
    _min_value = *std::min_element(std::begin(_values), std::end(_values));
    _max_value = *std::max_element(std::begin(_values), std::end(_values));
    _in_between = static_cast<int>(_min_value + 0.5 * (_max_value - _min_value));
  }

  pmr_vector<int> _values;
  int _min_value, _max_value, _in_between;
};

TEST_F(PruningFiltersTest, MinMaxFilterTest) {
  auto filter = std::make_unique<MinMaxFilter<int>>(_values.front(), _values.back());

  EXPECT_TRUE(filter->can_prune({_min_value - 1}, PredicateCondition::LessThan));
  EXPECT_TRUE(filter->can_prune({_min_value}, PredicateCondition::LessThan));
  EXPECT_FALSE(filter->can_prune({_in_between}, PredicateCondition::LessThan));
  EXPECT_FALSE(filter->can_prune({_max_value}, PredicateCondition::LessThan));
  EXPECT_FALSE(filter->can_prune({_max_value + 1}, PredicateCondition::LessThan));

  EXPECT_TRUE(filter->can_prune({_min_value - 1}, PredicateCondition::LessThanEquals));
  EXPECT_FALSE(filter->can_prune({_min_value}, PredicateCondition::LessThanEquals));
  EXPECT_FALSE(filter->can_prune({_in_between}, PredicateCondition::LessThanEquals));
  EXPECT_FALSE(filter->can_prune({_max_value}, PredicateCondition::LessThanEquals));
  EXPECT_FALSE(filter->can_prune({_max_value + 1}, PredicateCondition::LessThanEquals));

  EXPECT_TRUE(filter->can_prune({_min_value - 1}, PredicateCondition::Equals));
  EXPECT_FALSE(filter->can_prune({_min_value}, PredicateCondition::Equals));
  EXPECT_FALSE(filter->can_prune({_in_between}, PredicateCondition::Equals));
  EXPECT_FALSE(filter->can_prune({_max_value}, PredicateCondition::Equals));
  EXPECT_TRUE(filter->can_prune({_max_value + 1}, PredicateCondition::Equals));

  EXPECT_FALSE(filter->can_prune({_min_value - 1}, PredicateCondition::GreaterThanEquals));
  EXPECT_FALSE(filter->can_prune({_min_value}, PredicateCondition::GreaterThanEquals));
  EXPECT_FALSE(filter->can_prune({_in_between}, PredicateCondition::GreaterThanEquals));
  EXPECT_FALSE(filter->can_prune({_max_value}, PredicateCondition::GreaterThanEquals));
  EXPECT_TRUE(filter->can_prune({_max_value + 1}, PredicateCondition::GreaterThanEquals));

  EXPECT_FALSE(filter->can_prune({_min_value - 1}, PredicateCondition::GreaterThan));
  EXPECT_FALSE(filter->can_prune({_min_value}, PredicateCondition::GreaterThan));
  EXPECT_FALSE(filter->can_prune({_in_between}, PredicateCondition::GreaterThan));
  EXPECT_TRUE(filter->can_prune({_max_value}, PredicateCondition::GreaterThan));
  EXPECT_TRUE(filter->can_prune({_max_value + 1}, PredicateCondition::GreaterThan));

  EXPECT_FALSE(filter->can_prune({_in_between}, PredicateCondition::IsNull));
}

TEST_F(PruningFiltersTest, RangeFilterGapTest) {
  auto filter = RangeFilter<int>::build_filter(_values);

  EXPECT_EQ(true, filter->can_prune({5}, PredicateCondition::Equals));
  EXPECT_EQ(true, filter->can_prune({9}, PredicateCondition::Equals));
}

TEST_F(PruningFiltersTest, RangeFilterExtremesTest) {
  auto filter = RangeFilter<int>::build_filter(_values);

  EXPECT_EQ(true, filter->can_prune({5}, PredicateCondition::Equals));
  EXPECT_EQ(true, filter->can_prune({9}, PredicateCondition::Equals));
  EXPECT_EQ(true, filter->can_prune({_max_value + 1}, PredicateCondition::Equals));
  EXPECT_EQ(true, filter->can_prune({_max_value + 1}, PredicateCondition::GreaterThan));
  EXPECT_EQ(true, filter->can_prune({_min_value - 1}, PredicateCondition::LessThan));
}

TEST_F(PruningFiltersTest, RangeFilterFloatTest) {
  pmr_vector<float> values = {1.f, 3.f, 458.7f};
  auto filter = RangeFilter<float>::build_filter(values);

  EXPECT_EQ(true, filter->can_prune({2.f}, PredicateCondition::Equals));
  EXPECT_EQ(false, filter->can_prune({458.7f}, PredicateCondition::Equals));
  EXPECT_EQ(true, filter->can_prune({700.f}, PredicateCondition::GreaterThan));
  EXPECT_EQ(true, filter->can_prune({-5.f}, PredicateCondition::LessThan));
}

}  // namespace opossum
