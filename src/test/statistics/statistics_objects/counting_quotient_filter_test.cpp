#include <memory>
#include <set>
#include <string>
#include <type_traits>
#include <unordered_set>
#include <utility>
#include <vector>

#include "base_test.hpp"

#include "statistics/statistics_objects/counting_quotient_filter.hpp"
#include "storage/base_segment.hpp"
#include "storage/chunk.hpp"
#include "types.hpp"
#include "utils/assert.hpp"

namespace {

using namespace opossum;  // NOLINT

template <typename T>
std::map<T, size_t> value_counts() {
  Fail("There should be a specialization for this");
}

template <>
std::map<int32_t, size_t> value_counts<int32_t>() {
  return {{-17, 3}, {1, 54}, {12, 43}, {123, 32}, {1'234, 21}, {12'345, 8}, {123'456, 6}};
}

template <>
std::map<int64_t, size_t> value_counts<int64_t>() {
  return {{-100'000'000'000, 3}, {100'000ll, 54},      {1'200'000ll, 43},    {12'300'000ll, 32},
          {123'400'000ll, 21},   {1'234'500'000ll, 8}, {12'345'600'000ll, 6}};
}

template <>
std::map<pmr_string, size_t> value_counts<pmr_string>() {
  return {{"hotel", 1}, {"delta", 6}, {"frank", 2}, {"apple", 9}, {"charlie", 3}, {"inbox", 1}};
}

template <typename T>
T get_test_value(size_t run) {
  Fail("There should be a specialization for this");
}

template <>
int32_t get_test_value<int32_t>(size_t run) {
  return static_cast<int32_t>(123457 + run);
}
template <>
int64_t get_test_value<int64_t>(size_t run) {
  return static_cast<int64_t>(123457 + run);
}

template <>
pmr_string get_test_value<pmr_string>(size_t run) {
  return pmr_string{std::string("test_value") + std::to_string(run)};
}

}  // namespace

namespace opossum {

template <typename T>
class CountingQuotientFilterTypedTest : public BaseTest {
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
      EXPECT_FALSE(cqf->does_not_contain(value_and_count.first));
    }
  }

  void test_false_positive_rate(const std::shared_ptr<CountingQuotientFilter<T>>& cqf) {
    size_t runs = 1000;
    size_t false_positives = 0;
    for (size_t run = 0; run < runs; ++run) {
      auto test_value = get_test_value<T>(run);
      if (!cqf->does_not_contain(test_value)) {
        ++false_positives;
      }
    }
    const auto false_positive_rate = false_positives / static_cast<float>(runs);
    EXPECT_LT(false_positive_rate, 0.4f);
  }
};

using Types = ::testing::Types<int32_t, int64_t, pmr_string>;
TYPED_TEST_SUITE(CountingQuotientFilterTypedTest, Types, );  // NOLINT(whitespace/parens)

TYPED_TEST(CountingQuotientFilterTypedTest, ValueCounts) {
  this->test_value_counts(this->cqf2);
  this->test_value_counts(this->cqf4);
  this->test_value_counts(this->cqf8);
  this->test_value_counts(this->cqf16);
  this->test_value_counts(this->cqf32);
}

TYPED_TEST(CountingQuotientFilterTypedTest, CanNotPrune) {
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
TYPED_TEST(CountingQuotientFilterTypedTest, FalsePositiveRate) {
  this->test_false_positive_rate(this->cqf2);
  this->test_false_positive_rate(this->cqf4);
  this->test_false_positive_rate(this->cqf8);
  this->test_false_positive_rate(this->cqf16);
  this->test_false_positive_rate(this->cqf32);
}

// Testing the get_hash_bits functions which is used in the CQF.
TYPED_TEST(CountingQuotientFilterTypedTest, HashBits) {
  for (auto bit_count : {8, 16, 32, 64}) {
    if constexpr (std::is_arithmetic<TypeParam>::value) {
      for (auto numeric_value : {-1'132'323'323, -28, -0, 0, 17, 32'323'323}) {
        const auto return_value =
            CountingQuotientFilter<TypeParam>::get_hash_bits(static_cast<TypeParam>(numeric_value), bit_count);
        EXPECT_GE(return_value, 0);
        EXPECT_LT(return_value, std::pow(2, bit_count));
      }
    } else {
      for (auto& string_value : {"alpha", "beta", "charlie", "zeier"}) {
        const auto return_value = CountingQuotientFilter<TypeParam>::get_hash_bits(string_value, bit_count);
        EXPECT_GE(return_value, 0);
        EXPECT_LT(return_value, std::pow(2, bit_count));
      }
    }
  }
}

class CountingQuotientFilterTest : public BaseTest {};

// Floating point types are not supported.
TEST_F(CountingQuotientFilterTest, FloatingPointTypesUnsupported) {
  EXPECT_NO_THROW(CountingQuotientFilter<int>(4, 4));

  EXPECT_THROW(CountingQuotientFilter<float>(4, 4), std::logic_error);
  EXPECT_THROW(CountingQuotientFilter<double>(4, 4), std::logic_error);
}

TEST_F(CountingQuotientFilterTest, QuotientSizes) {
  // Quotient needs to be larger than zero.
  EXPECT_THROW(CountingQuotientFilter<int>(0, 4), std::logic_error);

  // Sum of quotient and remainder should not exceed 64 bit.
  EXPECT_THROW(CountingQuotientFilter<int>(32, 33), std::logic_error);
}

}  // namespace opossum
