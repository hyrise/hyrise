#include <memory>
#include <set>
#include <string>
#include <unordered_set>
#include <utility>
#include <vector>

#include "base_test.hpp"
#include "gtest/gtest.h"

#include "statistics/chunk_statistics/counting_quotient_filter.hpp"
#include "storage/base_segment.hpp"
#include "storage/chunk.hpp"
#include "types.hpp"

namespace opossum {

class CountingQuotientFilterTest : public BaseTest {
 protected:
  void SetUp() override {
    string_value_counts["hotel"] = 1;
    string_value_counts["delta"] = 6;
    string_value_counts["frank"] = 2;
    string_value_counts["apple"] = 9;
    string_value_counts["charlie"] = 3;
    string_value_counts["inbox"] = 1;
    string_segment = std::make_shared<ValueSegment<std::string>>();
    for (auto value_and_count : string_value_counts) {
      for (size_t i = 0; i < value_and_count.second; i++) {
        string_segment->append(value_and_count.first);
      }
    }

    int_value_counts[1] = 54;
    int_value_counts[12] = 43;
    int_value_counts[123] = 32;
    int_value_counts[1234] = 21;
    int_value_counts[12345] = 8;
    int_value_counts[123456] = 6;
    int_segment = std::make_shared<ValueSegment<int>>();
    for (auto value_and_count : int_value_counts) {
      for (size_t i = 0; i < value_and_count.second; i++) {
        int_segment->append(value_and_count.first);
      }
    }

    string_cqf2 = std::make_shared<CountingQuotientFilter<std::string>>(4, 2);
    string_cqf4 = std::make_shared<CountingQuotientFilter<std::string>>(4, 4);
    string_cqf8 = std::make_shared<CountingQuotientFilter<std::string>>(4, 8);
    string_cqf16 = std::make_shared<CountingQuotientFilter<std::string>>(4, 16);
    string_cqf32 = std::make_shared<CountingQuotientFilter<std::string>>(4, 32);
    string_cqf2->populate(string_segment);
    string_cqf4->populate(string_segment);
    string_cqf8->populate(string_segment);
    string_cqf16->populate(string_segment);
    string_cqf32->populate(string_segment);

    int_cqf2 = std::make_shared<CountingQuotientFilter<int>>(4, 2);
    int_cqf4 = std::make_shared<CountingQuotientFilter<int>>(4, 4);
    int_cqf8 = std::make_shared<CountingQuotientFilter<int>>(4, 8);
    int_cqf16 = std::make_shared<CountingQuotientFilter<int>>(4, 16);
    int_cqf32 = std::make_shared<CountingQuotientFilter<int>>(4, 32);
    int_cqf2->populate(int_segment);
    int_cqf4->populate(int_segment);
    int_cqf8->populate(int_segment);
    int_cqf16->populate(int_segment);
    int_cqf32->populate(int_segment);
  }

  std::shared_ptr<CountingQuotientFilter<std::string>> string_cqf2;
  std::shared_ptr<CountingQuotientFilter<std::string>> string_cqf4;
  std::shared_ptr<CountingQuotientFilter<std::string>> string_cqf8;
  std::shared_ptr<CountingQuotientFilter<std::string>> string_cqf16;
  std::shared_ptr<CountingQuotientFilter<std::string>> string_cqf32;
  std::shared_ptr<CountingQuotientFilter<int>> int_cqf2;
  std::shared_ptr<CountingQuotientFilter<int>> int_cqf4;
  std::shared_ptr<CountingQuotientFilter<int>> int_cqf8;
  std::shared_ptr<CountingQuotientFilter<int>> int_cqf16;
  std::shared_ptr<CountingQuotientFilter<int>> int_cqf32;
  std::shared_ptr<ValueSegment<std::string>> string_segment;
  std::shared_ptr<ValueSegment<int>> int_segment;
  std::map<std::string, size_t> string_value_counts;
  std::map<int, size_t> int_value_counts;

  template <typename DataType>
  void test_value_counts(std::shared_ptr<CountingQuotientFilter<DataType>> cqf,
                         std::map<DataType, size_t> value_counts) {
    for (auto value_and_count : value_counts) {
      EXPECT_TRUE(cqf->count(value_and_count.first) >= value_and_count.second);
    }
  }

  template <typename DataType>
  void test_can_not_prune(std::shared_ptr<CountingQuotientFilter<DataType>> cqf,
                          std::map<DataType, size_t> value_counts) {
    for (auto value_and_count : value_counts) {
      EXPECT_FALSE(cqf->can_prune(PredicateCondition::Equals, value_and_count.first));
    }
  }

  template <typename DataType>
  DataType get_test_value(size_t run) {
    throw std::logic_error("specialize this method");
  }

  template <typename DataType>
  void test_false_positive_rate(std::shared_ptr<CountingQuotientFilter<DataType>> cqf) {
    size_t runs = 1000;
    size_t false_positives = 0;
    for (size_t run = 0; run < runs; run++) {
      auto test_value = get_test_value<DataType>(run);
      if (!cqf->can_prune(PredicateCondition::Equals, test_value)) {
        false_positives++;
      }
    }
    double false_positive_rate = false_positives / static_cast<double>(runs);
    EXPECT_TRUE(false_positive_rate < 0.1);
  }
};

template <>
int CountingQuotientFilterTest::get_test_value<int>(size_t run) {
  return 123457 + run;
}

template <>
std::string CountingQuotientFilterTest::get_test_value<std::string>(size_t run) {
  return std::string("test_value") + std::to_string(run);
}

TEST_F(CountingQuotientFilterTest, NoUndercountsString) {
  test_value_counts<std::string>(string_cqf2, string_value_counts);
  test_value_counts<std::string>(string_cqf4, string_value_counts);
  test_value_counts<std::string>(string_cqf8, string_value_counts);
  test_value_counts<std::string>(string_cqf16, string_value_counts);
  test_value_counts<std::string>(string_cqf32, string_value_counts);
}

TEST_F(CountingQuotientFilterTest, NoUndercountsInt) {
  test_value_counts<int>(int_cqf2, int_value_counts);
  test_value_counts<int>(int_cqf4, int_value_counts);
  test_value_counts<int>(int_cqf8, int_value_counts);
  test_value_counts<int>(int_cqf16, int_value_counts);
  test_value_counts<int>(int_cqf32, int_value_counts);
}

TEST_F(CountingQuotientFilterTest, CanNotPruneString) {
  test_can_not_prune<std::string>(string_cqf2, string_value_counts);
  test_can_not_prune<std::string>(string_cqf4, string_value_counts);
  test_can_not_prune<std::string>(string_cqf8, string_value_counts);
  test_can_not_prune<std::string>(string_cqf16, string_value_counts);
  test_can_not_prune<std::string>(string_cqf32, string_value_counts);
}

TEST_F(CountingQuotientFilterTest, CanNotPruneInt) {
  test_can_not_prune<int>(int_cqf2, int_value_counts);
  test_can_not_prune<int>(int_cqf4, int_value_counts);
  test_can_not_prune<int>(int_cqf8, int_value_counts);
  test_can_not_prune<int>(int_cqf16, int_value_counts);
  test_can_not_prune<int>(int_cqf32, int_value_counts);
}

TEST_F(CountingQuotientFilterTest, FalsePositiveRateInt) {
  test_false_positive_rate<int>(int_cqf2);
  test_false_positive_rate<int>(int_cqf4);
  test_false_positive_rate<int>(int_cqf8);
  test_false_positive_rate<int>(int_cqf16);
  test_false_positive_rate<int>(int_cqf32);
}

TEST_F(CountingQuotientFilterTest, FalsePositiveRateString) {
  test_false_positive_rate<std::string>(string_cqf2);
  test_false_positive_rate<std::string>(string_cqf4);
  test_false_positive_rate<std::string>(string_cqf8);
  test_false_positive_rate<std::string>(string_cqf16);
  test_false_positive_rate<std::string>(string_cqf32);
}

}  // namespace opossum
