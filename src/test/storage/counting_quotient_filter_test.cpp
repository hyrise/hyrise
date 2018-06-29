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
    for (auto key : value_counts) {
      for (int i = 0; i < values_counts[key]; i++) {
        column.append(key);
      }
    }
    cqf = std::make_shared<CountingQuotientFilter<std::string>>(3, 8);
    cqf->populate(column);
  }

  std::shared_ptr<CountingQuotientFilter<std::string>> cqf;
  std::shared_ptr<ValueColumn<std::string>>
  std::map<std::string, uint>> value_counts;
};

TEST_F(CountingQuotientFilter, Probes) {

}

}  // namespace opossum
