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
  void SetUp() override { _values = pmr_vector<int>{2, 3, 4, 7, 8, 10}; }

  pmr_vector<int> _values;
};

TEST_F(PruningFiltersTest, MinMaxFilterTest) {
  auto filter = std::make_unique<MinMaxFilter<int>>(_values.front(), _values.back());

  EXPECT_EQ(true, filter->can_prune(PredicateCondition::Equals, {1}));
  EXPECT_EQ(true, filter->can_prune(PredicateCondition::GreaterThan, {42}));
  EXPECT_EQ(true, filter->can_prune(PredicateCondition::LessThanEquals, {-21}));
}

TEST_F(PruningFiltersTest, RangeFilterGapTest) {
  auto filter = RangeFilter<int>::build_filter(_values);

  EXPECT_EQ(true, filter->can_prune(PredicateCondition::Equals, {5}));
  EXPECT_EQ(true, filter->can_prune(PredicateCondition::Equals, {9}));
}

TEST_F(PruningFiltersTest, RangeFilterExtremesTest) {
  auto filter = RangeFilter<int>::build_filter(_values);

  EXPECT_EQ(true, filter->can_prune(PredicateCondition::Equals, {5}));
  EXPECT_EQ(true, filter->can_prune(PredicateCondition::Equals, {9}));
  EXPECT_EQ(true, filter->can_prune(PredicateCondition::Equals, {21}));
  EXPECT_EQ(true, filter->can_prune(PredicateCondition::GreaterThan, {42}));
  EXPECT_EQ(true, filter->can_prune(PredicateCondition::LessThan, {-5}));
}

TEST_F(PruningFiltersTest, RangeFilterFloatTest) {
  pmr_vector<float> values = {1.f, 3.f, 458.7f};
  auto filter = RangeFilter<float>::build_filter(values);

  EXPECT_EQ(true, filter->can_prune(PredicateCondition::Equals, {2.f}));
  EXPECT_EQ(false, filter->can_prune(PredicateCondition::Equals, {458.7f}));
  EXPECT_EQ(true, filter->can_prune(PredicateCondition::GreaterThan, {700.f}));
  EXPECT_EQ(true, filter->can_prune(PredicateCondition::LessThan, {-5.f}));
}

}  // namespace opossum
