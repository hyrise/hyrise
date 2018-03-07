#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "../../base_test.hpp"
#include "gtest/gtest.h"

#include "utils/assert.hpp"

#include "optimizer/chunk_statistics/min_max_filter.hpp"
#include "optimizer/chunk_statistics/range_filter.hpp"
#include "types.hpp"

namespace opossum {

class PruningFiltersTest : public BaseTest {
 protected:
  void SetUp() override { _values = pmr_vector<int>{2, 3, 4, 7, 8, 10}; }

  pmr_vector<int> _values;
};

TEST_F(PruningFiltersTest, MinMaxFilterTest) {
  auto filter = std::make_unique<MinMaxFilter<int>>(_values.front(), _values.back());

  EXPECT_EQ(true, filter->can_prune({1}, PredicateCondition::Equals));
  EXPECT_EQ(true, filter->can_prune({42}, PredicateCondition::GreaterThan));
  EXPECT_EQ(true, filter->can_prune({-21}, PredicateCondition::LessThanEquals));
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
  EXPECT_EQ(true, filter->can_prune({21}, PredicateCondition::Equals));
  EXPECT_EQ(true, filter->can_prune({42}, PredicateCondition::GreaterThan));
  EXPECT_EQ(true, filter->can_prune({-5}, PredicateCondition::LessThan));
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
