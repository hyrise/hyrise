#include <memory>
#include <set>
#include <string>
#include <unordered_set>
#include <utility>
#include <vector>

#include "base_test.hpp"
#include "gtest/gtest.h"

#include "storage/base_column.hpp"
#include "storage/chunk.hpp"
#include "statistics/chunk_statistics/counting_quotient_filter.hpp"
#include "types.hpp"

namespace opossum {

class CountingQuotientFilterTest : public BaseTest {
 protected:
  void SetUp() override {
    value_counts["hotel"] = 1;
    value_counts["delta"] = 6;
    value_counts["frank"] = 2;
    value_counts["apple"] = 9;
    value_counts["charlie"] = 3;
    value_counts["inbox"] = 1;
    column = std::make_shared<ValueColumn<std::string>>();
    for (auto value_count : value_counts) {
      for (uint i = 0; i < value_count.second; i++) {
        column->append(value_count.first);
      }
    }

    cqf2 = std::make_shared<CountingQuotientFilter<std::string>>(4, RemainderSize::bits2);
    cqf4 = std::make_shared<CountingQuotientFilter<std::string>>(4, RemainderSize::bits4);
    cqf8 = std::make_shared<CountingQuotientFilter<std::string>>(4, RemainderSize::bits8);
    cqf16 = std::make_shared<CountingQuotientFilter<std::string>>(4, RemainderSize::bits16);
    cqf32 = std::make_shared<CountingQuotientFilter<std::string>>(4, RemainderSize::bits32);
    cqf2->populate(column);
    cqf4->populate(column);
    cqf8->populate(column);
    cqf16->populate(column);
    cqf32->populate(column);
  }

  std::shared_ptr<CountingQuotientFilter<std::string>> cqf2;
  std::shared_ptr<CountingQuotientFilter<std::string>> cqf4;
  std::shared_ptr<CountingQuotientFilter<std::string>> cqf8;
  std::shared_ptr<CountingQuotientFilter<std::string>> cqf16;
  std::shared_ptr<CountingQuotientFilter<std::string>> cqf32;
  std::shared_ptr<ValueColumn<std::string>> column;
  std::map<std::string, uint> value_counts;
};

TEST_F(CountingQuotientFilterTest, NoUndercounts2) {
  for (auto value_count : value_counts) {
      EXPECT_TRUE(cqf2->count(value_count.first) >= value_count.second);
  }
}

TEST_F(CountingQuotientFilterTest, NoUndercounts4) {
  for (auto value_count : value_counts) {
      EXPECT_TRUE(cqf4->count(value_count.first) >= value_count.second);
  }
}

TEST_F(CountingQuotientFilterTest, NoUndercounts8) {
  for (auto value_count : value_counts) {
      EXPECT_TRUE(cqf8->count(value_count.first) >= value_count.second);
  }
}

TEST_F(CountingQuotientFilterTest, NoUndercounts16) {
  for (auto value_count : value_counts) {
      EXPECT_TRUE(cqf16->count(value_count.first) >= value_count.second);
  }
}

TEST_F(CountingQuotientFilterTest, NoUndercounts32) {
  for (auto value_count : value_counts) {
      EXPECT_TRUE(cqf32->count(value_count.first) >= value_count.second);
  }
}

TEST_F(CountingQuotientFilterTest, CanPrune2) {
  for (auto value_count : value_counts) {
      EXPECT_FALSE(cqf2->can_prune(value_count.first, PredicateCondition::Equals));
  }
}

TEST_F(CountingQuotientFilterTest, CanPrune4) {
  for (auto value_count : value_counts) {
      EXPECT_FALSE(cqf4->can_prune(value_count.first, PredicateCondition::Equals));
  }
}

TEST_F(CountingQuotientFilterTest, CanPrune8) {
  for (auto value_count : value_counts) {
      EXPECT_FALSE(cqf8->can_prune(value_count.first, PredicateCondition::Equals));
  }
}

TEST_F(CountingQuotientFilterTest, CanPrune16) {
  for (auto value_count : value_counts) {
      EXPECT_FALSE(cqf16->can_prune(value_count.first, PredicateCondition::Equals));
  }
}

TEST_F(CountingQuotientFilterTest, CanPrune32) {
  for (auto value_count : value_counts) {
      EXPECT_FALSE(cqf32->can_prune(value_count.first, PredicateCondition::Equals));
  }
}

}  // namespace opossum
