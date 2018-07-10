#include <memory>
#include <set>
#include <string>
#include <unordered_set>
#include <utility>
#include <vector>

#include "../base_test.hpp"
#include "gtest/gtest.h"

#include "../lib/storage/base_column.hpp"
#include "../lib/storage/chunk.hpp"
#include "../lib/storage/index/counting_quotient_filter/counting_quotient_filter.hpp"
#include "../lib/types.hpp"

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
    cqf = std::make_shared<CountingQuotientFilter<std::string>>(4, 8);
    cqf->populate(column);
  }

  std::shared_ptr<CountingQuotientFilter<std::string>> cqf;
  std::shared_ptr<ValueColumn<std::string>> column;
  std::map<std::string, uint> value_counts;
};

TEST_F(CountingQuotientFilterTest, Probes) {
  for (auto value_count : value_counts) {
      EXPECT_TRUE(cqf->count(value_count.first) >= value_count.second);
  }
}

}  // namespace opossum
