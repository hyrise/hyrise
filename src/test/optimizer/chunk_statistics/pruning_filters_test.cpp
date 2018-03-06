#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "../../base_test.hpp"
#include "gtest/gtest.h"

#include "utils/assert.hpp"

#include "types.hpp"
#include "optimizer/chunk_statistics/min_max_filter.hpp"
#include "optimizer/chunk_statistics/range_filter.hpp"

namespace opossum {

class PruningFiltersTest : public BaseTest {
 protected:
  void SetUp() override {
    _values = pmr_vector<int>{2, 3, 4, 7, 8, 10};
  }

  pmr_vector<int> _values;
};

TEST_F(PruningFiltersTest, MinMaxFilterTest) {
  auto filter = std::make_unique<MinMaxFilter<int>>(_values.front(), _values.back());

  EXPECT_EQ(true, filter->can_prune({1}, PredicateCondition::Equals));
  EXPECT_EQ(true, filter->can_prune({42}, PredicateCondition::GreaterThan));
  EXPECT_EQ(true, filter->can_prune({-21}, PredicateCondition::LessThanEquals));
}

}  // namespace opossum
