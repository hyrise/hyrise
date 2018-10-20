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
#include "utils/assert.hpp"

namespace {

template <typename T>
std::map<T, size_t> value_counts() {
  opossum::Fail("There should be a specialization for this");
}

template <>
std::map<int32_t, size_t> value_counts<int32_t>() {
  return {{1, 54}, {12, 43}, {123, 32}, {1234, 21}, {12345, 8}, {123456, 6}};
}

template <>
std::map<int64_t, size_t> value_counts<int64_t>() {
  return {{100000ll, 54}, {1200000ll, 43}, {12300000ll, 32}, {123400000ll, 21}, {1234500000ll, 8}, {12345600000ll, 6}};
}

template <>
std::map<float, size_t> value_counts<float>() {
  return {{1.1f, 54}, {12.2f, 43}, {123.3f, 32}, {1234.4f, 21}, {12345.5f, 8}, {123456.6f, 6}};
}

template <>
std::map<double, size_t> value_counts<double>() {
  return {{1.1, 54}, {12.2, 43}, {123.3, 32}, {1234.4, 21}, {12345.5, 8}, {123456.6, 6}};
}

template <>
std::map<std::string, size_t> value_counts<std::string>() {
  return {{"hotel", 1}, {"delta", 6}, {"frank", 2}, {"apple", 9}, {"charlie", 3}, {"inbox", 1}};
}

template <typename T>
T get_test_value(size_t run) {
  opossum::Fail("There should be a specialization for this");
}

template <>
int32_t get_test_value<int32_t>(size_t run) {
  return 123457 + run;
}
template <>
int64_t get_test_value<int64_t>(size_t run) {
  return 123457 + run;
}
template <>
float get_test_value<float>(size_t run) {
  return 123457.0f + run;
}
template <>
double get_test_value<double>(size_t run) {
  return 123457.0 + run;
}
template <>
std::string get_test_value<std::string>(size_t run) {
  return std::string("test_value") + std::to_string(run);
}

}  // namespace

namespace opossum {

template <typename T>
class CountingQuotientFilterTest : public BaseTest {
 protected:
  void SetUp() override {
    this->value_counts = ::value_counts<T>();

    segment = std::make_shared<ValueSegment<T>>();
    for (auto value_and_count : value_counts) {
      for (size_t i = 0; i < value_and_count.second; i++) {
        segment->append(value_and_count.first);
      }
    }

    cqf2 = std::make_shared<CountingQuotientFilter<T>>(4, 2);
    cqf4 = std::make_shared<CountingQuotientFilter<T>>(4, 4);
    cqf8 = std::make_shared<CountingQuotientFilter<T>>(4, 8);
    cqf16 = std::make_shared<CountingQuotientFilter<T>>(4, 16);
    cqf32 = std::make_shared<CountingQuotientFilter<T>>(4, 32);
    cqf2->populate(segment);
    cqf4->populate(segment);
    cqf8->populate(segment);
    cqf16->populate(segment);
    cqf32->populate(segment);
  }

  std::shared_ptr<CountingQuotientFilter<T>> cqf2;
  std::shared_ptr<CountingQuotientFilter<T>> cqf4;
  std::shared_ptr<CountingQuotientFilter<T>> cqf8;
  std::shared_ptr<CountingQuotientFilter<T>> cqf16;
  std::shared_ptr<CountingQuotientFilter<T>> cqf32;
  std::shared_ptr<ValueSegment<T>> segment;
  std::map<T, size_t> value_counts;

  void test_value_counts(const std::shared_ptr<CountingQuotientFilter<T>>& cqf) {
    for (const auto& value_and_count : value_counts) {
      EXPECT_TRUE(cqf->count(value_and_count.first) >= value_and_count.second);
    }
  }

  void test_can_not_prune(const std::shared_ptr<CountingQuotientFilter<T>>& cqf) {
    for (const auto& value_and_count : value_counts) {
      EXPECT_FALSE(cqf->can_prune(PredicateCondition::Equals, value_and_count.first));
    }
  }

  void test_false_positive_rate(const std::shared_ptr<CountingQuotientFilter<T>>& cqf) {
    size_t runs = 1000;
    size_t false_positives = 0;
    for (size_t run = 0; run < runs; ++run) {
      auto test_value = get_test_value<T>(run);
      if (!cqf->can_prune(PredicateCondition::Equals, test_value)) {
        ++false_positives;
      }
    }
    const auto false_positive_rate = false_positives / static_cast<float>(runs);
    EXPECT_TRUE(false_positive_rate < 0.5f);
  }
};

using Types = ::testing::Types<int32_t, int64_t, float, double, std::string>;
TYPED_TEST_CASE(CountingQuotientFilterTest, Types);

TYPED_TEST(CountingQuotientFilterTest, ValueCounts) {
  this->test_value_counts(this->cqf2);
  this->test_value_counts(this->cqf4);
  this->test_value_counts(this->cqf8);
  this->test_value_counts(this->cqf16);
  this->test_value_counts(this->cqf32);
}

TYPED_TEST(CountingQuotientFilterTest, CanNotPrune) {
  this->test_can_not_prune(this->cqf2);
  this->test_can_not_prune(this->cqf4);
  this->test_can_not_prune(this->cqf8);
  this->test_can_not_prune(this->cqf16);
  this->test_can_not_prune(this->cqf32);
}

/**
 * There is no guarantee on the False Positive Rate of CountingQuotientFilter::does_contain(). These tests perform some
 * sanity checking, making sure the FPR is below a very lenient threshold. If these tests fail it is very likely,
 * however not absolutely certain, that there is a bug in the CQF.
 */
TYPED_TEST(CountingQuotientFilterTest, FalsePositiveRate) {
  this->test_false_positive_rate(this->cqf2);
  this->test_false_positive_rate(this->cqf4);
  this->test_false_positive_rate(this->cqf8);
  this->test_false_positive_rate(this->cqf16);
  this->test_false_positive_rate(this->cqf32);
}

}  // namespace opossum
