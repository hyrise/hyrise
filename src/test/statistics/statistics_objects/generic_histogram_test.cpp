#include <limits>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "base_test.hpp"
#include "gtest/gtest.h"

#include "constant_mappings.hpp"
#include "statistics/statistics_objects/generic_histogram.hpp"
#include "utils/load_table.hpp"

/**
 * As well as testing GenericHistogram, we also test a lot of the functionally implemented in AbstractHistogram here.
 * We do this here since GenericHistogram allows us to easily construct interesting edge cases.
 */

namespace {

using namespace opossum;  // NOLINT

struct Predicate {
  PredicateCondition predicate_condition;
  AllTypeVariant value;
  std::optional<AllTypeVariant> value2;
};

template <typename T>
T next_value(T v) {
  return HistogramDomain<T>{}.next_value_clamped(v);
}

template <typename T>
T previous_value(T v) {
  return HistogramDomain<T>{}.previous_value_clamped(v);
}

}  // namespace

namespace opossum {

class GenericHistogramTest : public BaseTest {
 public:
  std::string predicate_to_string(const Predicate& predicate) {
    std::ostringstream stream;
    stream << predicate_condition_to_string.left.at(predicate.predicate_condition) << " " << predicate.value;
    if (predicate.value2) {
      stream << " " << *predicate.value2;
    }

    return stream.str();
  }

  template <typename T>
  void test_sliced_with_predicates(const std::vector<std::shared_ptr<AbstractHistogram<T>>>& histograms,
                                   const std::vector<Predicate>& predicates) {
    for (const auto& predicate : predicates) {
      SCOPED_TRACE(predicate_to_string(predicate));

      for (const auto& histogram : histograms) {
        SCOPED_TRACE(histogram->description());

        const auto sliced_statistics_object =
            histogram->sliced(predicate.predicate_condition, predicate.value, predicate.value2);
        const auto cardinality =
            histogram->estimate_cardinality(predicate.predicate_condition, predicate.value, predicate.value2);

        if (const auto sliced_histogram = std::dynamic_pointer_cast<AbstractHistogram<T>>(sliced_statistics_object)) {
          SCOPED_TRACE(sliced_histogram->description());
          EXPECT_NEAR(sliced_histogram->total_count(), cardinality, 0.005f);
        } else {
          EXPECT_EQ(cardinality, 0.0f);
        }
      }
    }
  }
};

TEST_F(GenericHistogramTest, EstimateCardinalityAndPruningBasicInt) {
  const auto histogram = GenericHistogram<int32_t>{{12, 12345}, {123, 123456}, {2, 5}, {2, 2}};
  EXPECT_FLOAT_EQ(histogram.estimate_cardinality(PredicateCondition::Equals, 0), 0.f);
  EXPECT_FLOAT_EQ(histogram.estimate_cardinality(PredicateCondition::Equals, 12), 1.f);
  EXPECT_FLOAT_EQ(histogram.estimate_cardinality(PredicateCondition::Equals, 1'234), 0.f);
  EXPECT_FLOAT_EQ(histogram.estimate_cardinality(PredicateCondition::Equals, 123'456), 2.5f);
  EXPECT_FLOAT_EQ(histogram.estimate_cardinality(PredicateCondition::Equals, 1'000'000), 0.f);
}

TEST_F(GenericHistogramTest, EstimateCardinalityAndPruningBasicFloat) {
  const auto histogram = GenericHistogram<float>{{0.5f, 2.5f, 3.6f}, {2.2f, 3.3f, 6.1f}, {4, 6, 4}, {4, 3, 3}};

  EXPECT_FLOAT_EQ(histogram.estimate_cardinality(PredicateCondition::Equals, 0.4f), 0.f);
  EXPECT_FLOAT_EQ(histogram.estimate_cardinality(PredicateCondition::Equals, 0.5f), 4 / 4.f);
  EXPECT_FLOAT_EQ(histogram.estimate_cardinality(PredicateCondition::Equals, 1.1f), 4 / 4.f);
  EXPECT_FLOAT_EQ(histogram.estimate_cardinality(PredicateCondition::Equals, 1.3f), 4 / 4.f);
  EXPECT_FLOAT_EQ(histogram.estimate_cardinality(PredicateCondition::Equals, 2.2f), 4 / 4.f);
  EXPECT_FLOAT_EQ(histogram.estimate_cardinality(PredicateCondition::Equals, 2.3f), 0.f);
  EXPECT_FLOAT_EQ(histogram.estimate_cardinality(PredicateCondition::Equals, 2.5f), 6 / 3.f);
  EXPECT_FLOAT_EQ(histogram.estimate_cardinality(PredicateCondition::Equals, 2.9f), 6 / 3.f);
  EXPECT_FLOAT_EQ(histogram.estimate_cardinality(PredicateCondition::Equals, 3.3f), 6 / 3.f);
  EXPECT_FLOAT_EQ(histogram.estimate_cardinality(PredicateCondition::Equals, 3.5f), 0.f);
  EXPECT_FLOAT_EQ(histogram.estimate_cardinality(PredicateCondition::Equals, 3.6f), 4 / 3.f);
  EXPECT_FLOAT_EQ(histogram.estimate_cardinality(PredicateCondition::Equals, 3.9f), 4 / 3.f);
  EXPECT_FLOAT_EQ(histogram.estimate_cardinality(PredicateCondition::Equals, 6.1f), 4 / 3.f);
  EXPECT_FLOAT_EQ(histogram.estimate_cardinality(PredicateCondition::Equals, 6.2f), 0.f);
}

TEST_F(GenericHistogramTest, EstimateCardinalityAndPruningBasicString) {
  const auto histogram = GenericHistogram<pmr_string>{
      {"aa", "bla", "uuu", "yyy"}, {"birne", "ttt", "xxx", "zzz"}, {3, 4, 4, 4}, {3, 3, 3, 2}, StringHistogramDomain{}};
  EXPECT_FLOAT_EQ(histogram.estimate_cardinality(PredicateCondition::Equals, "a"), 0.f);
  EXPECT_FLOAT_EQ(histogram.estimate_cardinality(PredicateCondition::Equals, "aa"), 3 / 3.f);
  EXPECT_FLOAT_EQ(histogram.estimate_cardinality(PredicateCondition::Equals, "ab"), 3 / 3.f);
  EXPECT_FLOAT_EQ(histogram.estimate_cardinality(PredicateCondition::Equals, "b"), 3 / 3.f);
  EXPECT_FLOAT_EQ(histogram.estimate_cardinality(PredicateCondition::Equals, "birne"), 3 / 3.f);
  EXPECT_FLOAT_EQ(histogram.estimate_cardinality(PredicateCondition::Equals, "biscuit"), 0.f);
  EXPECT_FLOAT_EQ(histogram.estimate_cardinality(PredicateCondition::Equals, "bla"), 4 / 3.f);
  EXPECT_FLOAT_EQ(histogram.estimate_cardinality(PredicateCondition::Equals, "blubb"), 4 / 3.f);
  EXPECT_FLOAT_EQ(histogram.estimate_cardinality(PredicateCondition::Equals, "bums"), 4 / 3.f);
  EXPECT_FLOAT_EQ(histogram.estimate_cardinality(PredicateCondition::Equals, "ttt"), 4 / 3.f);
  EXPECT_FLOAT_EQ(histogram.estimate_cardinality(PredicateCondition::Equals, "turkey"), 0.f);
  EXPECT_FLOAT_EQ(histogram.estimate_cardinality(PredicateCondition::Equals, "uuu"), 4 / 3.f);
  EXPECT_FLOAT_EQ(histogram.estimate_cardinality(PredicateCondition::Equals, "vvv"), 4 / 3.f);
  EXPECT_FLOAT_EQ(histogram.estimate_cardinality(PredicateCondition::Equals, "www"), 4 / 3.f);
  EXPECT_FLOAT_EQ(histogram.estimate_cardinality(PredicateCondition::Equals, "xxx"), 4 / 3.f);
  EXPECT_FLOAT_EQ(histogram.estimate_cardinality(PredicateCondition::Equals, "yyy"), 4 / 2.f);
  EXPECT_FLOAT_EQ(histogram.estimate_cardinality(PredicateCondition::Equals, "zzz"), 4 / 2.f);
  EXPECT_FLOAT_EQ(histogram.estimate_cardinality(PredicateCondition::Equals, "zzzzzz"), 0.f);
}

TEST_F(GenericHistogramTest, FloatLessThan) {
  const auto histogram = GenericHistogram<float>{{0.5f, 2.5f, 3.6f}, {2.2f, 3.3f, 6.1f}, {4, 6, 4}, {4, 3, 3}};

  EXPECT_FLOAT_EQ(histogram.estimate_cardinality(PredicateCondition::LessThan, 0.5f), 0.f);

  EXPECT_FLOAT_EQ(histogram.estimate_cardinality(PredicateCondition::LessThan, 1.0f),
                  (1.0f - 0.5f) / std::nextafter(2.2f - 0.5f, std::numeric_limits<float>::infinity()) * 4);

  EXPECT_FLOAT_EQ(histogram.estimate_cardinality(PredicateCondition::LessThan, 1.7f),
                  (1.7f - 0.5f) / std::nextafter(2.2f - 0.5f, std::numeric_limits<float>::infinity()) * 4);

  EXPECT_FLOAT_EQ(histogram.estimate_cardinality(PredicateCondition::LessThan,
                                                 std::nextafter(2.2f, std::numeric_limits<float>::infinity())),
                  4.f);

  EXPECT_FLOAT_EQ(histogram.estimate_cardinality(PredicateCondition::LessThan, 2.5f), 4.f);

  EXPECT_FLOAT_EQ(histogram.estimate_cardinality(PredicateCondition::LessThan, 3.0f),
                  4.f + (3.0f - 2.5f) / std::nextafter(3.3f - 2.5f, std::numeric_limits<float>::infinity()) * 6);

  EXPECT_FLOAT_EQ(histogram.estimate_cardinality(PredicateCondition::LessThan, 3.3f),
                  4.f + (3.3f - 2.5f) / std::nextafter(3.3f - 2.5f, std::numeric_limits<float>::infinity()) * 6);

  EXPECT_FLOAT_EQ(histogram.estimate_cardinality(PredicateCondition::LessThan,
                                                 std::nextafter(3.3f, std::numeric_limits<float>::infinity())),
                  4.f + 6.f);

  EXPECT_FLOAT_EQ(histogram.estimate_cardinality(PredicateCondition::LessThan, 3.6f), 4.f + 6.f);

  EXPECT_FLOAT_EQ(histogram.estimate_cardinality(PredicateCondition::LessThan, 3.9f),
                  4.f + 6.f + (3.9f - 3.6f) / std::nextafter(6.1f - 3.6f, std::numeric_limits<float>::infinity()) * 4);

  EXPECT_FLOAT_EQ(histogram.estimate_cardinality(PredicateCondition::LessThan, 5.9f),
                  4.f + 6.f + (5.9f - 3.6f) / std::nextafter(6.1f - 3.6f, std::numeric_limits<float>::infinity()) * 4);

  EXPECT_FLOAT_EQ(histogram.estimate_cardinality(PredicateCondition::LessThan,
                                                 std::nextafter(6.1f, std::numeric_limits<float>::infinity())),
                  4.f + 6.f + 4.f);
}

TEST_F(GenericHistogramTest, StringLessThan) {
  const auto histogram = GenericHistogram<pmr_string>{{"abcd", "ijkl", "oopp", "uvwx"},
                                                      {"efgh", "mnop", "qrst", "yyzz"},
                                                      {4, 6, 3, 3},
                                                      {3, 3, 3, 3},
                                                      StringHistogramDomain{'a', 'z', 4u}};

  // "abcd"
  const auto bin_1_lower = 0 * (ipow(26, 3) + ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1 +
                           1 * (ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1 + 2 * (ipow(26, 1) + ipow(26, 0)) + 1 +
                           3 * (ipow(26, 0)) + 1;
  // "efgh"
  const auto bin_1_upper = 4 * (ipow(26, 3) + ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1 +
                           5 * (ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1 + 6 * (ipow(26, 1) + ipow(26, 0)) + 1 +
                           7 * (ipow(26, 0)) + 1;
  // "ijkl"
  const auto bin_2_lower = 8 * (ipow(26, 3) + ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1 +
                           9 * (ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1 + 10 * (ipow(26, 1) + ipow(26, 0)) + 1 +
                           11 * (ipow(26, 0)) + 1;
  // "mnop"
  const auto bin_2_upper = 12 * (ipow(26, 3) + ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1 +
                           13 * (ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1 + 14 * (ipow(26, 1) + ipow(26, 0)) + 1 +
                           15 * (ipow(26, 0)) + 1;
  // "oopp"
  const auto bin_3_lower = 14 * (ipow(26, 3) + ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1 +
                           14 * (ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1 + 15 * (ipow(26, 1) + ipow(26, 0)) + 1 +
                           15 * (ipow(26, 0)) + 1;
  // "qrst"
  const auto bin_3_upper = 16 * (ipow(26, 3) + ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1 +
                           17 * (ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1 + 18 * (ipow(26, 1) + ipow(26, 0)) + 1 +
                           19 * (ipow(26, 0)) + 1;
  // "uvwx"
  const auto bin_4_lower = 20 * (ipow(26, 3) + ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1 +
                           21 * (ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1 + 22 * (ipow(26, 1) + ipow(26, 0)) + 1 +
                           23 * (ipow(26, 0)) + 1;
  // "yyzz"
  const auto bin_4_upper = 24 * (ipow(26, 3) + ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1 +
                           24 * (ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1 + 25 * (ipow(26, 1) + ipow(26, 0)) + 1 +
                           25 * (ipow(26, 0)) + 1;

  const auto bin_1_width = (bin_1_upper - bin_1_lower + 1.f);
  const auto bin_2_width = (bin_2_upper - bin_2_lower + 1.f);
  const auto bin_3_width = (bin_3_upper - bin_3_lower + 1.f);
  const auto bin_4_width = (bin_4_upper - bin_4_lower + 1.f);

  constexpr auto bin_1_count = 4.f;
  constexpr auto bin_2_count = 6.f;
  constexpr auto bin_3_count = 3.f;
  constexpr auto bin_4_count = 3.f;
  constexpr auto total_count = bin_1_count + bin_2_count + bin_3_count + bin_4_count;

  EXPECT_FLOAT_EQ(histogram.estimate_cardinality(PredicateCondition::LessThan, "aaaa"), 0.f);

  EXPECT_FLOAT_EQ(histogram.estimate_cardinality(PredicateCondition::LessThan, "abcd"), 0.f);

  EXPECT_FLOAT_EQ(histogram.estimate_cardinality(PredicateCondition::LessThan, "abce"), 1 / bin_1_width * bin_1_count);

  EXPECT_FLOAT_EQ(histogram.estimate_cardinality(PredicateCondition::LessThan, "abcf"), 2 / bin_1_width * bin_1_count);

  EXPECT_FLOAT_EQ(
      histogram.estimate_cardinality(PredicateCondition::LessThan, "cccc"),
      (2 * (ipow(26, 3) + ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1 + 2 * (ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) +
       1 + 2 * (ipow(26, 1) + ipow(26, 0)) + 1 + 2 * (ipow(26, 0)) + 1 - bin_1_lower) /
          bin_1_width * bin_1_count);

  EXPECT_FLOAT_EQ(
      histogram.estimate_cardinality(PredicateCondition::LessThan, "dddd"),
      (3 * (ipow(26, 3) + ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1 + 3 * (ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) +
       1 + 3 * (ipow(26, 1) + ipow(26, 0)) + 1 + 3 * (ipow(26, 0)) + 1 - bin_1_lower) /
          bin_1_width * bin_1_count);

  EXPECT_FLOAT_EQ(histogram.estimate_cardinality(PredicateCondition::LessThan, "efgg"),
                  (bin_1_width - 2) / bin_1_width * bin_1_count);

  EXPECT_FLOAT_EQ(histogram.estimate_cardinality(PredicateCondition::LessThan, "efgh"),
                  (bin_1_width - 1) / bin_1_width * bin_1_count);

  EXPECT_FLOAT_EQ(histogram.estimate_cardinality(PredicateCondition::LessThan, "efgi"), bin_1_count);
  EXPECT_FLOAT_EQ(histogram.estimate_cardinality(PredicateCondition::LessThan, "ijkl"), bin_1_count);
  EXPECT_FLOAT_EQ(histogram.estimate_cardinality(PredicateCondition::LessThan, "ijkm"),
                  1 / bin_2_width * bin_2_count + bin_1_count);

  EXPECT_FLOAT_EQ(histogram.estimate_cardinality(PredicateCondition::LessThan, "ijkn"),
                  2 / bin_2_width * bin_2_count + bin_1_count);

  EXPECT_FLOAT_EQ(
      histogram.estimate_cardinality(PredicateCondition::LessThan, "jjjj"),
      (9 * (ipow(26, 3) + ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1 + 9 * (ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) +
       1 + 9 * (ipow(26, 1) + ipow(26, 0)) + 1 + 9 * (ipow(26, 0)) + 1 - bin_2_lower) /
              bin_2_width * bin_2_count +
          bin_1_count);

  EXPECT_FLOAT_EQ(histogram.estimate_cardinality(PredicateCondition::LessThan, "kkkk"),
                  (10 * (ipow(26, 3) + ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1 +
                   10 * (ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1 + 10 * (ipow(26, 1) + ipow(26, 0)) + 1 +
                   10 * (ipow(26, 0)) + 1 - bin_2_lower) /
                          bin_2_width * bin_2_count +
                      bin_1_count);

  EXPECT_FLOAT_EQ(histogram.estimate_cardinality(PredicateCondition::LessThan, "lzzz"),
                  (11 * (ipow(26, 3) + ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1 +
                   25 * (ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1 + 25 * (ipow(26, 1) + ipow(26, 0)) + 1 +
                   25 * (ipow(26, 0)) + 1 - bin_2_lower) /
                          bin_2_width * bin_2_count +
                      bin_1_count);

  EXPECT_FLOAT_EQ(histogram.estimate_cardinality(PredicateCondition::LessThan, "mnoo"),
                  (bin_2_width - 2) / bin_2_width * bin_2_count + bin_1_count);

  EXPECT_FLOAT_EQ(histogram.estimate_cardinality(PredicateCondition::LessThan, "mnop"),
                  (bin_2_width - 1) / bin_2_width * bin_2_count + bin_1_count);
  EXPECT_FLOAT_EQ(histogram.estimate_cardinality(PredicateCondition::LessThan, "mnoq"), bin_1_count + bin_2_count);
  EXPECT_FLOAT_EQ(histogram.estimate_cardinality(PredicateCondition::LessThan, "oopp"), bin_1_count + bin_2_count);

  EXPECT_FLOAT_EQ(histogram.estimate_cardinality(PredicateCondition::LessThan, "oopq"),
                  1 / bin_3_width * bin_3_count + bin_1_count + bin_2_count);

  EXPECT_FLOAT_EQ(histogram.estimate_cardinality(PredicateCondition::LessThan, "oopr"),
                  2 / bin_3_width * bin_3_count + bin_1_count + bin_2_count);

  EXPECT_FLOAT_EQ(histogram.estimate_cardinality(PredicateCondition::LessThan, "pppp"),
                  (15 * (ipow(26, 3) + ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1 +
                   15 * (ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1 + 15 * (ipow(26, 1) + ipow(26, 0)) + 1 +
                   15 * (ipow(26, 0)) + 1 - bin_3_lower) /
                          bin_3_width * bin_3_count +
                      bin_1_count + bin_2_count);

  EXPECT_FLOAT_EQ(histogram.estimate_cardinality(PredicateCondition::LessThan, "qqqq"),
                  (16 * (ipow(26, 3) + ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1 +
                   16 * (ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1 + 16 * (ipow(26, 1) + ipow(26, 0)) + 1 +
                   16 * (ipow(26, 0)) + 1 - bin_3_lower) /
                          bin_3_width * bin_3_count +
                      bin_1_count + bin_2_count);

  EXPECT_FLOAT_EQ(histogram.estimate_cardinality(PredicateCondition::LessThan, "qllo"),
                  (16 * (ipow(26, 3) + ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1 +
                   11 * (ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1 + 11 * (ipow(26, 1) + ipow(26, 0)) + 1 +
                   14 * (ipow(26, 0)) + 1 - bin_3_lower) /
                          bin_3_width * bin_3_count +
                      bin_1_count + bin_2_count);

  EXPECT_FLOAT_EQ(histogram.estimate_cardinality(PredicateCondition::LessThan, "qrss"),
                  (bin_3_width - 2) / bin_3_width * bin_3_count + bin_1_count + bin_2_count);

  EXPECT_FLOAT_EQ(histogram.estimate_cardinality(PredicateCondition::LessThan, "qrst"),
                  (bin_3_width - 1) / bin_3_width * bin_3_count + bin_1_count + bin_2_count);

  EXPECT_FLOAT_EQ(histogram.estimate_cardinality(PredicateCondition::LessThan, "qrsu"),
                  bin_1_count + bin_2_count + bin_3_count);

  EXPECT_FLOAT_EQ(histogram.estimate_cardinality(PredicateCondition::LessThan, "uvwx"),
                  bin_1_count + bin_2_count + bin_3_count);

  EXPECT_FLOAT_EQ(histogram.estimate_cardinality(PredicateCondition::LessThan, "uvwy"),
                  1 / bin_4_width * bin_4_count + bin_1_count + bin_2_count + bin_3_count);

  EXPECT_FLOAT_EQ(histogram.estimate_cardinality(PredicateCondition::LessThan, "uvwz"),
                  2 / bin_4_width * bin_4_count + bin_1_count + bin_2_count + bin_3_count);

  EXPECT_FLOAT_EQ(histogram.estimate_cardinality(PredicateCondition::LessThan, "vvvv"),
                  (21 * (ipow(26, 3) + ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1 +
                   21 * (ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1 + 21 * (ipow(26, 1) + ipow(26, 0)) + 1 +
                   21 * (ipow(26, 0)) + 1 - bin_4_lower) /
                          bin_4_width * bin_4_count +
                      bin_1_count + bin_2_count + bin_3_count);

  EXPECT_FLOAT_EQ(histogram.estimate_cardinality(PredicateCondition::LessThan, "xxxx"),
                  (23 * (ipow(26, 3) + ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1 +
                   23 * (ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1 + 23 * (ipow(26, 1) + ipow(26, 0)) + 1 +
                   23 * (ipow(26, 0)) + 1 - bin_4_lower) /
                          bin_4_width * bin_4_count +
                      bin_1_count + bin_2_count + bin_3_count);

  EXPECT_FLOAT_EQ(histogram.estimate_cardinality(PredicateCondition::LessThan, "ycip"),
                  (24 * (ipow(26, 3) + ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1 +
                   2 * (ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1 + 8 * (ipow(26, 1) + ipow(26, 0)) + 1 +
                   15 * (ipow(26, 0)) + 1 - bin_4_lower) /
                          bin_4_width * bin_4_count +
                      bin_1_count + bin_2_count + bin_3_count);

  EXPECT_FLOAT_EQ(histogram.estimate_cardinality(PredicateCondition::LessThan, "yyzy"),
                  (bin_4_width - 2) / bin_4_width * bin_4_count + bin_1_count + bin_2_count + bin_3_count);

  EXPECT_FLOAT_EQ(histogram.estimate_cardinality(PredicateCondition::LessThan, "yyzz"),
                  (bin_4_width - 1) / bin_4_width * bin_4_count + bin_1_count + bin_2_count + bin_3_count);

  EXPECT_FLOAT_EQ(histogram.estimate_cardinality(PredicateCondition::LessThan, "yz"), total_count);

  EXPECT_FLOAT_EQ(histogram.estimate_cardinality(PredicateCondition::LessThan, "zzzz"), total_count);
}

TEST_F(GenericHistogramTest, StringLikeEstimation) {
  const auto histogram =
      GenericHistogram<pmr_string>::with_single_bin("a", "z", 100, 40, StringHistogramDomain{'a', 'z', 4u});

  // (NOT) LIKE is not estimated and has a selectivity of 1
  EXPECT_FLOAT_EQ(histogram->estimate_cardinality(PredicateCondition::Like, "aa_zz%"), 100.f);
  EXPECT_FLOAT_EQ(histogram->estimate_cardinality(PredicateCondition::NotLike, "aa_zz%"), 100.f);
}

TEST_F(GenericHistogramTest, EstimateCardinalityInt) {
  // clang-format off
  const auto histogram = GenericHistogram<int32_t>(
    {2,  21, 37,  101, 105},
    {20, 25, 100, 103, 105},
    {17, 30, 40,  1,     5},
    { 5,  3, 27,  1,     1});

  const auto histogram_zeros = GenericHistogram<int32_t>(
    {2,    21,     37},
    {20,   25,    100},
    {0.0f,  6.0f,   0.0f},
    {5.0f,  0.0f,   0.0f});
  // clang-format on

  const auto total_count = histogram.total_count();

  // clang-format off
  EXPECT_EQ(histogram.estimate_cardinality(PredicateCondition::Equals, 1), 0.0f);
  EXPECT_EQ(histogram.estimate_cardinality(PredicateCondition::Equals, 3), 17.0f / 5.0f);
  EXPECT_EQ(histogram.estimate_cardinality(PredicateCondition::Equals, 26), 0.0f);
  EXPECT_EQ(histogram.estimate_cardinality(PredicateCondition::Equals, 105), 5.0f);
  EXPECT_EQ(histogram.estimate_cardinality(PredicateCondition::Equals, 200), 0.0f);
  EXPECT_EQ(histogram_zeros.estimate_cardinality(PredicateCondition::Equals, 2), 0.0f);
  EXPECT_EQ(histogram_zeros.estimate_cardinality(PredicateCondition::Equals, 21), 0.0f);
  EXPECT_EQ(histogram_zeros.estimate_cardinality(PredicateCondition::Equals, 37), 0.0f);

  EXPECT_EQ(histogram.estimate_cardinality(PredicateCondition::NotEquals, 1), total_count);
  EXPECT_EQ(histogram.estimate_cardinality(PredicateCondition::NotEquals, 21), total_count - 10);
  EXPECT_EQ(histogram_zeros.estimate_cardinality(PredicateCondition::NotEquals, 2), 6.0f);
  EXPECT_EQ(histogram_zeros.estimate_cardinality(PredicateCondition::NotEquals, 21), 6.0f);
  EXPECT_EQ(histogram_zeros.estimate_cardinality(PredicateCondition::NotEquals, 37), 6.0f);

  EXPECT_EQ(histogram.estimate_cardinality(PredicateCondition::LessThan, -10), 0.0f);
  EXPECT_EQ(histogram.estimate_cardinality(PredicateCondition::LessThan, 2), 0.0f);
  EXPECT_EQ(histogram.estimate_cardinality(PredicateCondition::LessThan, 20), 17.0f - 17.0f / 19.0f);  // NOLINT
  EXPECT_EQ(histogram.estimate_cardinality(PredicateCondition::LessThan, 21), 17.0f);
  EXPECT_EQ(histogram.estimate_cardinality(PredicateCondition::LessThan, 40), 17.0f + 30 + 3 * (40.0f / 64.0f));  // NOLINT
  EXPECT_EQ(histogram.estimate_cardinality(PredicateCondition::LessThan, 105), total_count - 5);
  EXPECT_EQ(histogram.estimate_cardinality(PredicateCondition::LessThan, 1000), total_count);

  EXPECT_EQ(histogram.estimate_cardinality(PredicateCondition::LessThanEquals, -10), 0.0f);
  EXPECT_EQ(histogram.estimate_cardinality(PredicateCondition::LessThanEquals, 2), 17.0f / 19.0f);
  EXPECT_EQ(histogram.estimate_cardinality(PredicateCondition::LessThanEquals, 3), 2 * (17.0f / 19.0f));  // NOLINT
  EXPECT_EQ(histogram.estimate_cardinality(PredicateCondition::LessThanEquals, 20), 17.0f);
  EXPECT_EQ(histogram.estimate_cardinality(PredicateCondition::LessThanEquals, 21), 17.0f + (30.0f / 5.0f));  // NOLINT
  EXPECT_EQ(histogram.estimate_cardinality(PredicateCondition::LessThanEquals, 40), 17.0f + 30 + 4 * (40.0f / 64.0f));  // NOLINT
  EXPECT_EQ(histogram.estimate_cardinality(PredicateCondition::LessThanEquals, 105), total_count);  // NOLINT
  EXPECT_EQ(histogram.estimate_cardinality(PredicateCondition::LessThanEquals, 1000), total_count);  // NOLINT

  EXPECT_EQ(histogram.estimate_cardinality(PredicateCondition::GreaterThan, -10), total_count);
  EXPECT_EQ(histogram.estimate_cardinality(PredicateCondition::GreaterThan, 1), total_count);
  EXPECT_EQ(histogram.estimate_cardinality(PredicateCondition::GreaterThan, 2), total_count - (17.0f / 19.0f));  // NOLINT
  EXPECT_EQ(histogram.estimate_cardinality(PredicateCondition::GreaterThan, 20), 76.0f);
  EXPECT_EQ(histogram.estimate_cardinality(PredicateCondition::GreaterThan, 21), 76.0f - (30.0f / 5.0f));  // NOLINT
  EXPECT_EQ(histogram.estimate_cardinality(PredicateCondition::GreaterThan, 105), 0.0f);
  EXPECT_EQ(histogram.estimate_cardinality(PredicateCondition::GreaterThan, 1000), 0.0f);

  EXPECT_EQ(histogram.estimate_cardinality(PredicateCondition::GreaterThanEquals, -10), total_count);  // NOLINT
  EXPECT_EQ(histogram.estimate_cardinality(PredicateCondition::GreaterThanEquals, 1), total_count);  // NOLINT
  EXPECT_EQ(histogram.estimate_cardinality(PredicateCondition::GreaterThanEquals, 2), total_count);  // NOLINT
  EXPECT_EQ(histogram.estimate_cardinality(PredicateCondition::GreaterThanEquals, 20), 76.0f + 17.0f / 19.0f);  // NOLINT
  EXPECT_EQ(histogram.estimate_cardinality(PredicateCondition::GreaterThanEquals, 21), 76.0f);
  EXPECT_EQ(histogram.estimate_cardinality(PredicateCondition::GreaterThanEquals, 105), 5.0f);
  EXPECT_EQ(histogram.estimate_cardinality(PredicateCondition::GreaterThanEquals, 1000), 0.0f);

  EXPECT_EQ(histogram.estimate_cardinality(PredicateCondition::BetweenInclusive, 2, 20), 17.0f);
  EXPECT_EQ(histogram.estimate_cardinality(PredicateCondition::BetweenInclusive, 2, 25), 47.0f);
  EXPECT_EQ(histogram.estimate_cardinality(PredicateCondition::BetweenInclusive, 26, 27), 0.0f);
  EXPECT_EQ(histogram.estimate_cardinality(PredicateCondition::BetweenInclusive, 105, 105), 5);
  EXPECT_EQ(histogram.estimate_cardinality(PredicateCondition::BetweenInclusive, 105, 106), 5);
  EXPECT_EQ(histogram.estimate_cardinality(PredicateCondition::BetweenInclusive, 107, 107), 0.0f);
  // clang-format on
}

TEST_F(GenericHistogramTest, EstimateCardinalityFloat) {
  // clang-format off
  const auto histogram = std::make_shared<GenericHistogram<float>>(
    std::vector<float>             {2.0f,  23.0f, next_value(25.0f),            31.0f,  32.0f},
    std::vector<float>             {22.0f, 25.0f,            30.0f,  next_value(31.0f), 32.0f},
    std::vector<HistogramCountType>{17,    30,               20,                 7,      3},
    std::vector<HistogramCountType>{ 5,     3,                5,                 2,      1});
  // clang-format on

  const auto total_count = histogram->total_count();

  // clang-format off
  EXPECT_EQ(histogram->estimate_cardinality(PredicateCondition::Equals, 1.0f), 0.0f);
  EXPECT_EQ(histogram->estimate_cardinality(PredicateCondition::Equals, 3.0f), 17.f / 5.0f);
  EXPECT_EQ(histogram->estimate_cardinality(PredicateCondition::Equals, 22.5f), 0.0f);
  EXPECT_EQ(histogram->estimate_cardinality(PredicateCondition::Equals, 31.0f), 7.0f / 2.0f);
  EXPECT_EQ(histogram->estimate_cardinality(PredicateCondition::Equals, next_value(31.0f)), 7.0f / 2.0f);
  EXPECT_EQ(histogram->estimate_cardinality(PredicateCondition::Equals, 32.0f), 3.0f);

  EXPECT_EQ(histogram->estimate_cardinality(PredicateCondition::NotEquals, 1.0f), total_count);
  EXPECT_EQ(histogram->estimate_cardinality(PredicateCondition::NotEquals, 2.0f), total_count - 17.0f / 5.0f);  // NOLINT
  EXPECT_EQ(histogram->estimate_cardinality(PredicateCondition::NotEquals, 31.0f), total_count - 7.0f / 2.0f);  // NOLINT
  EXPECT_EQ(histogram->estimate_cardinality(PredicateCondition::NotEquals, next_value(31.0f)), total_count - 7.0f / 2.0f);  // NOLINT
  EXPECT_EQ(histogram->estimate_cardinality(PredicateCondition::NotEquals, 32.0f), 74);

  EXPECT_EQ(histogram->estimate_cardinality(PredicateCondition::LessThan, 2.0f), 0.0f);
  EXPECT_EQ(histogram->estimate_cardinality(PredicateCondition::LessThan, next_value(2.0f)), 17.0f * ((next_value(2.0f) - 2.0f) / 20.0f));  // NOLINT
  // Floating point quirk: These go to exactly 67, should be slightly below
  EXPECT_EQ(histogram->estimate_cardinality(PredicateCondition::LessThan, 24.5f), 17.0f + 30 * (1.5f / 2.0f)); // NOLINT
  EXPECT_EQ(histogram->estimate_cardinality(PredicateCondition::LessThan, 30.0f), 67.0f);
  EXPECT_EQ(histogram->estimate_cardinality(PredicateCondition::LessThan, next_value(30.0f)), 67.0f);
  EXPECT_EQ(histogram->estimate_cardinality(PredicateCondition::LessThan, 150.0f), total_count);
  EXPECT_EQ(histogram->estimate_cardinality(PredicateCondition::LessThan, 31.0f), 67.0f);
  // Floating point quirk: The bin [31, next_value(31.0f)] is too small to be split at `< next_value(31.0f)`
  EXPECT_EQ(histogram->estimate_cardinality(PredicateCondition::LessThan, next_value(31.0f)), 74.0f);
  EXPECT_EQ(histogram->estimate_cardinality(PredicateCondition::LessThan, 32.0f), 74.0f);
  EXPECT_EQ(histogram->estimate_cardinality(PredicateCondition::LessThan, next_value(32.0f)), 77.0f);

  EXPECT_EQ(histogram->estimate_cardinality(PredicateCondition::LessThanEquals, 2.0f), 17.0f * ((next_value(2.0f) - 2.0f) / 20.0f));  // NOLINT
  EXPECT_EQ(histogram->estimate_cardinality(PredicateCondition::LessThanEquals, previous_value(24.0f)), 17.0f + 30.0f * 0.5f);  // NOLINT
  EXPECT_EQ(histogram->estimate_cardinality(PredicateCondition::LessThanEquals, 30.0f), 67.0f);
  EXPECT_EQ(histogram->estimate_cardinality(PredicateCondition::LessThanEquals, 150.0f), total_count);
  // Floating point quirk: `<= 31.0f` is `< next_value(31.0f)`, which in turn covers the entire bin.
  EXPECT_EQ(histogram->estimate_cardinality(PredicateCondition::LessThanEquals, 31.0f), 74.0f);
  EXPECT_EQ(histogram->estimate_cardinality(PredicateCondition::LessThanEquals, next_value(31.0f)), 74.0f);  // NOLINT
  EXPECT_EQ(histogram->estimate_cardinality(PredicateCondition::LessThanEquals, 32.0f), total_count);
  EXPECT_EQ(histogram->estimate_cardinality(PredicateCondition::LessThanEquals, next_value(32.0f)), total_count);  // NOLINT

  EXPECT_EQ(histogram->estimate_cardinality(PredicateCondition::GreaterThan, 2.0f), total_count - 17.0f * ((next_value(2.0f) - 2.0f) / 20.0f));  // NOLINT
  EXPECT_EQ(histogram->estimate_cardinality(PredicateCondition::GreaterThan, 30.0f), 10.0f);
  EXPECT_EQ(histogram->estimate_cardinality(PredicateCondition::GreaterThan, 150.0f), 0.0f);
  EXPECT_EQ(histogram->estimate_cardinality(PredicateCondition::GreaterThan, 31.0f), 3.0f);
  EXPECT_EQ(histogram->estimate_cardinality(PredicateCondition::GreaterThan, next_value(31.0f)), 3.0f);  // NOLINT
  EXPECT_EQ(histogram->estimate_cardinality(PredicateCondition::GreaterThan, 32.0f), 0.0f);
  EXPECT_EQ(histogram->estimate_cardinality(PredicateCondition::GreaterThan, previous_value(32.0f)), 3);  // NOLINT

  EXPECT_EQ(histogram->estimate_cardinality(PredicateCondition::GreaterThanEquals, 2.0f), total_count);  // NOLINT
  EXPECT_EQ(histogram->estimate_cardinality(PredicateCondition::GreaterThanEquals, 24.0f), 45.0f);
  EXPECT_EQ(histogram->estimate_cardinality(PredicateCondition::GreaterThanEquals, 30.0f), 10.0f);
  EXPECT_EQ(histogram->estimate_cardinality(PredicateCondition::GreaterThanEquals, 150.0f), 0.0f);
  EXPECT_EQ(histogram->estimate_cardinality(PredicateCondition::GreaterThanEquals, 31.0f), 10.0f);
  EXPECT_EQ(histogram->estimate_cardinality(PredicateCondition::GreaterThanEquals, next_value(31.0f)), 3.0f);  // NOLINT
  EXPECT_EQ(histogram->estimate_cardinality(PredicateCondition::GreaterThanEquals, 32.0f), 3.0f);
  EXPECT_EQ(histogram->estimate_cardinality(PredicateCondition::GreaterThanEquals, previous_value(32.0f)), 3);  // NOLINT

  EXPECT_FLOAT_EQ(histogram->estimate_cardinality(PredicateCondition::BetweenInclusive, 2.0f, 3.0f),  17.0f * ((next_value(3.0f) - 2.0f) / 20.0f));  // NOLINT
  EXPECT_FLOAT_EQ(histogram->estimate_cardinality(PredicateCondition::BetweenInclusive, 2.0f, next_value(2.0f)), 0.0f);  // NOLINT
  EXPECT_EQ(histogram->estimate_cardinality(PredicateCondition::BetweenInclusive, 2.0f, 22.5f), 17.0f);
  EXPECT_EQ(histogram->estimate_cardinality(PredicateCondition::BetweenInclusive, 2.0f, 30.0f), 67.0f);
  EXPECT_FLOAT_EQ(histogram->estimate_cardinality(PredicateCondition::BetweenInclusive, previous_value(2.0f), 2.0f), 0.0f);  // NOLINT
  // clang-format on
}

TEST_F(GenericHistogramTest, DoesNotContain) {
  // clang-format off
  const auto histogram_a = GenericHistogram<float>{
  std::vector<float>             {2.0f,  23.0f, next_value(25.0f),            31.0f,  32.0f},
  std::vector<float>             {22.0f, 25.0f,            30.0f,  next_value(31.0f), 32.0f},
  std::vector<HistogramCountType>{17,    30,               20,                 7,      3},
  std::vector<HistogramCountType>{ 5,     3,                5,                 2,      1}};
  // clang-format on

  EXPECT_TRUE(histogram_a.does_not_contain(PredicateCondition::Equals, 1.0f));
  EXPECT_TRUE(histogram_a.does_not_contain(PredicateCondition::Equals, 22.5f));
  EXPECT_TRUE(histogram_a.does_not_contain(PredicateCondition::Equals, 33.0f));
  EXPECT_FALSE(histogram_a.does_not_contain(PredicateCondition::Equals, 24.0f));
  EXPECT_FALSE(histogram_a.does_not_contain(PredicateCondition::Equals, 32.0f));

  EXPECT_FALSE(histogram_a.does_not_contain(PredicateCondition::NotEquals, 1.0f));
  EXPECT_FALSE(histogram_a.does_not_contain(PredicateCondition::NotEquals, 22.5f));
  EXPECT_FALSE(histogram_a.does_not_contain(PredicateCondition::NotEquals, 33.0f));

  EXPECT_TRUE(histogram_a.does_not_contain(PredicateCondition::GreaterThan, 32.0f));
  EXPECT_FALSE(histogram_a.does_not_contain(PredicateCondition::GreaterThan, 31.0f));

  EXPECT_TRUE(histogram_a.does_not_contain(PredicateCondition::GreaterThanEquals, 32.1f));
  EXPECT_FALSE(histogram_a.does_not_contain(PredicateCondition::GreaterThanEquals, 32.0f));

  EXPECT_TRUE(histogram_a.does_not_contain(PredicateCondition::LessThan, 2.0f));
  EXPECT_FALSE(histogram_a.does_not_contain(PredicateCondition::LessThan, 22.5f));

  EXPECT_TRUE(histogram_a.does_not_contain(PredicateCondition::LessThanEquals, 1.9f));
  EXPECT_FALSE(histogram_a.does_not_contain(PredicateCondition::LessThanEquals, 2.0f));
}

TEST_F(GenericHistogramTest, EstimateCardinalityString) {
  // clang-format off
  const auto histogram = std::make_shared<GenericHistogram<pmr_string>>(
  std::vector<pmr_string>        {"aa", "at", "bi"},
  std::vector<pmr_string>        {"as", "ax", "dr"},
  std::vector<HistogramCountType>{  17,   30,   40},
  std::vector<HistogramCountType>{   5,    3,   27},
  StringHistogramDomain{'a', 'z', 2u});
  // clang-format on

  auto estimate = histogram->estimate_cardinality(PredicateCondition::Equals, "a");
  EXPECT_FLOAT_EQ(estimate, 0.f);

  estimate = histogram->estimate_cardinality(PredicateCondition::Equals, "ab");
  EXPECT_FLOAT_EQ(estimate, 17.f / 5);

  estimate = histogram->estimate_cardinality(PredicateCondition::Equals, "ay");
  EXPECT_FLOAT_EQ(estimate, 0.f);
}

TEST_F(GenericHistogramTest, SlicedInt) {
  // clang-format off

  // Normal histogram, no special cases here
  const auto histogram_a = std::make_shared<GenericHistogram<int32_t>>(
    std::vector<int32_t>            { 1, 31, 60, 80},
    std::vector<int32_t>            {25, 50, 60, 99},
    std::vector<HistogramCountType> {40, 30, 5,  10},
    std::vector<HistogramCountType> {10, 20, 1,  1});

  // Histogram with zeros in bin height/distinct_count
  const auto histogram_b = std::make_shared<GenericHistogram<int32_t>>(
    std::vector<int32_t>            { 0,  6, 30, 51},
    std::vector<int32_t>            { 5, 20, 50, 52},
    std::vector<HistogramCountType> { 4,  0,  0,  0.000001f},
    std::vector<HistogramCountType> { 0, 20,  0,  0.000001f});

  // Histogram with a single bin, which in turn has a single value
  const auto histogram_c = std::make_shared<GenericHistogram<int32_t>>(
    std::vector<int32_t>            { 5},
    std::vector<int32_t>            {15},
    std::vector<HistogramCountType> { 1},
    std::vector<HistogramCountType> { 1});

  // Empty histogram
  const auto histogram_d = std::make_shared<GenericHistogram<int32_t>>(
    std::vector<int32_t>            {},
    std::vector<int32_t>            {},
    std::vector<HistogramCountType> {},
    std::vector<HistogramCountType> {});
  // clang-format on

  std::vector<Predicate> predicates{
      Predicate{PredicateCondition::Equals, -50, std::nullopt},
      Predicate{PredicateCondition::Equals, 5, std::nullopt},
      Predicate{PredicateCondition::Equals, 15, std::nullopt},
      Predicate{PredicateCondition::Equals, 60, std::nullopt},
      Predicate{PredicateCondition::Equals, 61, std::nullopt},
      Predicate{PredicateCondition::Equals, 150, std::nullopt},
      Predicate{PredicateCondition::NotEquals, 0, std::nullopt},
      Predicate{PredicateCondition::NotEquals, 26, std::nullopt},
      Predicate{PredicateCondition::NotEquals, 31, std::nullopt},
      Predicate{PredicateCondition::NotEquals, 36, std::nullopt},
      Predicate{PredicateCondition::NotEquals, 101, std::nullopt},
      Predicate{PredicateCondition::GreaterThan, 0, std::nullopt},
      Predicate{PredicateCondition::GreaterThan, 59, std::nullopt},
      Predicate{PredicateCondition::GreaterThan, 60, std::nullopt},
      Predicate{PredicateCondition::GreaterThan, 81, std::nullopt},
      Predicate{PredicateCondition::GreaterThan, 98, std::nullopt},
      Predicate{PredicateCondition::GreaterThanEquals, 1, std::nullopt},
      Predicate{PredicateCondition::GreaterThanEquals, 5, std::nullopt},
      Predicate{PredicateCondition::GreaterThanEquals, 60, std::nullopt},
      Predicate{PredicateCondition::GreaterThanEquals, 99, std::nullopt},
      Predicate{PredicateCondition::LessThan, 1, std::nullopt},
      Predicate{PredicateCondition::LessThan, 6, std::nullopt},
      Predicate{PredicateCondition::LessThan, 50, std::nullopt},
      Predicate{PredicateCondition::LessThan, 60, std::nullopt},
      Predicate{PredicateCondition::LessThan, 61, std::nullopt},
      Predicate{PredicateCondition::LessThan, 99, std::nullopt},
      Predicate{PredicateCondition::LessThan, 1000, std::nullopt},
      Predicate{PredicateCondition::LessThanEquals, 0, std::nullopt},
      Predicate{PredicateCondition::LessThanEquals, 2, std::nullopt},
      Predicate{PredicateCondition::LessThanEquals, 24, std::nullopt},
      Predicate{PredicateCondition::LessThanEquals, 25, std::nullopt},
      Predicate{PredicateCondition::LessThanEquals, 60, std::nullopt},
      Predicate{PredicateCondition::LessThanEquals, 97, std::nullopt},
      Predicate{PredicateCondition::LessThanEquals, 99, std::nullopt},
      Predicate{PredicateCondition::BetweenInclusive, 0, 0},
      Predicate{PredicateCondition::BetweenInclusive, 26, 29},
      Predicate{PredicateCondition::BetweenInclusive, 100, 1000},
      Predicate{PredicateCondition::BetweenInclusive, 1, 20},
      Predicate{PredicateCondition::BetweenInclusive, 1, 50},
      Predicate{PredicateCondition::BetweenInclusive, 21, 60},
      Predicate{PredicateCondition::BetweenInclusive, 60, 60},
      Predicate{PredicateCondition::BetweenInclusive, 32, 99},
  };

  std::vector<std::shared_ptr<AbstractHistogram<int32_t>>> histograms{histogram_a, histogram_b, histogram_c,
                                                                      histogram_d};

  test_sliced_with_predicates(histograms, predicates);
}

TEST_F(GenericHistogramTest, SliceReturnsNullptr) {
  const auto histogram = std::make_shared<GenericHistogram<int32_t>>(std::vector<int32_t>{1}, std::vector<int32_t>{25},
                                                                     std::vector<HistogramCountType>{40},
                                                                     std::vector<HistogramCountType>{10});

  // Check that histogram returns a nullptr if predicate will not match any data.
  EXPECT_EQ(histogram->sliced(PredicateCondition::LessThan, 1), nullptr);
  EXPECT_EQ(histogram->sliced(PredicateCondition::LessThanEquals, 0), nullptr);
  EXPECT_EQ(histogram->sliced(PredicateCondition::GreaterThanEquals, 26), nullptr);
  EXPECT_EQ(histogram->sliced(PredicateCondition::GreaterThan, 25), nullptr);
}

TEST_F(GenericHistogramTest, SlicedFloat) {
  // clang-format off
  const auto histogram_a = std::make_shared<GenericHistogram<float>>(
    std::vector<float>             {1.0f,            2.0f,   3.0f, 12.25f,       100.0f},
    std::vector<float>             {1.0f, next_value(2.0f), 11.0f, 17.25f, 1'000'100.0f},
    std::vector<HistogramCountType>{5,              10,     32,    70,             1},
    std::vector<HistogramCountType>{1,               5,      4,    70,             1});

  // Histogram with zeros in bin height/distinct_count
  const auto histogram_b = std::make_shared<GenericHistogram<float>>(
  std::vector<float>              { 0,  6, 30, 51},
  std::vector<float>              { 5, 20, 50, 52},
  std::vector<HistogramCountType> { 4,  0,  0,  0.000001f},
  std::vector<HistogramCountType> { 0, 20,  0,  0.000001f});

  const auto histogram_c = std::make_shared<GenericHistogram<float>>(
    std::vector<float>             {5.0f},
    std::vector<float>             {5.0f},
    std::vector<HistogramCountType>{12},
    std::vector<HistogramCountType>{1});

  const auto histogram_d = std::make_shared<GenericHistogram<float>>(
    std::vector<float>             {},
    std::vector<float>             {},
    std::vector<HistogramCountType>{},
    std::vector<HistogramCountType>{});
  // clang-format on

  std::vector<std::shared_ptr<AbstractHistogram<float>>> histograms{histogram_a, histogram_b, histogram_c, histogram_d};

  std::vector<Predicate> predicates{
      Predicate{PredicateCondition::Equals, 1.0f, std::nullopt},
      Predicate{PredicateCondition::Equals, 2.0f, std::nullopt},
      Predicate{PredicateCondition::Equals, next_value(2.0f), std::nullopt},
      Predicate{PredicateCondition::Equals, 4.2f, std::nullopt},
      Predicate{PredicateCondition::Equals, 5.0f, std::nullopt},
      Predicate{PredicateCondition::Equals, 11.1f, std::nullopt},
      Predicate{PredicateCondition::Equals, 5'000.0f, std::nullopt},
      Predicate{PredicateCondition::NotEquals, 0.0f, std::nullopt},
      Predicate{PredicateCondition::NotEquals, 1.0f, std::nullopt},
      Predicate{PredicateCondition::NotEquals, 2.0f, std::nullopt},
      Predicate{PredicateCondition::NotEquals, next_value(2.0f), std::nullopt},
      Predicate{PredicateCondition::NotEquals, 3.5f, std::nullopt},
      Predicate{PredicateCondition::NotEquals, 5.0f, std::nullopt},
      Predicate{PredicateCondition::NotEquals, 100'000'000.5f, std::nullopt},
      Predicate{PredicateCondition::LessThan, 1.0f, std::nullopt},
      Predicate{PredicateCondition::LessThan, next_value(1.0f), std::nullopt},
      Predicate{PredicateCondition::LessThan, 2.0f, std::nullopt},
      Predicate{PredicateCondition::LessThan, next_value(2.0f), std::nullopt},
      Predicate{PredicateCondition::LessThan, next_value(3.0f), std::nullopt},
      Predicate{PredicateCondition::LessThan, 5.0f, std::nullopt},
      Predicate{PredicateCondition::LessThan, 6.0f, std::nullopt},
      Predicate{PredicateCondition::LessThan, 7.0f, std::nullopt},
      Predicate{PredicateCondition::LessThanEquals, previous_value(0.0f), std::nullopt},
      Predicate{PredicateCondition::LessThanEquals, previous_value(1.0f), std::nullopt},
      Predicate{PredicateCondition::LessThanEquals, 1.0f, std::nullopt},
      Predicate{PredicateCondition::LessThanEquals, previous_value(2.0f), std::nullopt},
      Predicate{PredicateCondition::LessThanEquals, 2.0f, std::nullopt},
      Predicate{PredicateCondition::LessThanEquals, previous_value(6.0f), std::nullopt},
      Predicate{PredicateCondition::LessThanEquals, previous_value(11.0f), std::nullopt},
      Predicate{PredicateCondition::LessThanEquals, previous_value(12.25f), std::nullopt},
      Predicate{PredicateCondition::LessThanEquals, 12.25, std::nullopt},
      Predicate{PredicateCondition::LessThanEquals, 200.0f, std::nullopt},
      Predicate{PredicateCondition::LessThanEquals, 1'000'100.0f, std::nullopt},
      Predicate{PredicateCondition::GreaterThan, 0.0f, std::nullopt},
      Predicate{PredicateCondition::GreaterThan, 1.0f, std::nullopt},
      Predicate{PredicateCondition::GreaterThan, 1.5f, std::nullopt},
      Predicate{PredicateCondition::GreaterThan, 2.0f, std::nullopt},
      Predicate{PredicateCondition::GreaterThan, 6.0f, std::nullopt},
      Predicate{PredicateCondition::GreaterThan, 10.0f, std::nullopt},
      Predicate{PredicateCondition::GreaterThan, 10.1f, std::nullopt},
      Predicate{PredicateCondition::GreaterThan, 11.0f, std::nullopt},
      Predicate{PredicateCondition::GreaterThan, previous_value(1'000'100.0f), std::nullopt},
      Predicate{PredicateCondition::GreaterThanEquals, 0.0f, std::nullopt},
      Predicate{PredicateCondition::GreaterThanEquals, 1.0f, std::nullopt},
      Predicate{PredicateCondition::GreaterThanEquals, previous_value(1.0f), std::nullopt},
      Predicate{PredicateCondition::GreaterThanEquals, previous_value(2.0f), std::nullopt},
      Predicate{PredicateCondition::GreaterThanEquals, 11.0f, std::nullopt},
      Predicate{PredicateCondition::GreaterThanEquals, 200.0f, std::nullopt},
      Predicate{PredicateCondition::GreaterThanEquals, previous_value(1'000'100.0f), std::nullopt},
      Predicate{PredicateCondition::GreaterThanEquals, 1'000'100.0f, std::nullopt},
      Predicate{PredicateCondition::BetweenInclusive, 0.0f, 1'000'100.0f},
      Predicate{PredicateCondition::BetweenInclusive, 1.0f, 1'000'100.0f},
      Predicate{PredicateCondition::BetweenInclusive, 2.0f, 1'000'100.0f},
      Predicate{PredicateCondition::BetweenInclusive, 2.0f, 50.0f},
      Predicate{PredicateCondition::BetweenInclusive, 3.0f, 11.00f},
      Predicate{PredicateCondition::BetweenInclusive, 10.0f, 50.0f},
      Predicate{PredicateCondition::BetweenInclusive, 20.0f, 50.0f},
  };

  test_sliced_with_predicates(histograms, predicates);
}

TEST_F(GenericHistogramTest, PrunedInt) {
  // clang-format off
  const auto histogram = std::make_shared<GenericHistogram<int32_t>>(
    std::vector<int32_t>            { 1, 31, 60, 80},
    std::vector<int32_t>            {25, 50, 60, 99},
    std::vector<HistogramCountType> {40, 30, 5,  10},
    std::vector<HistogramCountType> {10, 20, 1,  1});
  // clang-format on

  {
    // Prune half of the values from the first bin
    const auto pruned = std::static_pointer_cast<GenericHistogram<int32_t>>(
        histogram->pruned(20, PredicateCondition::GreaterThanEquals, 26));

    // clang-format off
    const auto expected_histogram = GenericHistogram<int32_t>{
      std::vector<int32_t>            { 1, 31, 60, 80},
      std::vector<int32_t>            {25, 50, 60, 99},
      std::vector<HistogramCountType> {20, 30, 5,  10},  // This is the row that changes in the tests
      std::vector<HistogramCountType> {10, 20, 1,  1}};
    // clang-format on

    EXPECT_EQ(*pruned, expected_histogram);
  }

  {
    // Prune all values from the first bin and half of the values from the second bin
    const auto pruned =
        std::static_pointer_cast<GenericHistogram<int32_t>>(histogram->pruned(55, PredicateCondition::GreaterThan, 40));
    // clang-format off
    const auto expected_histogram = GenericHistogram<int32_t>{
      std::vector<int32_t>            {31, 60, 80},
      std::vector<int32_t>            {50, 60, 99},
      std::vector<HistogramCountType> {15, 5,  10},
      std::vector<HistogramCountType> {20, 1,  1}};
    // clang-format on

    EXPECT_EQ(*pruned, expected_histogram);
  }

  {
    // Prune three values from the last bin with the boundary being outside of any bin
    const auto pruned = std::static_pointer_cast<GenericHistogram<int32_t>>(
        histogram->pruned(3, PredicateCondition::LessThanEquals, 70));
    // clang-format off
    const auto expected_histogram = GenericHistogram<int32_t>{
      std::vector<int32_t>            { 1, 31, 60, 80},
      std::vector<int32_t>            {25, 50, 60, 99},
      std::vector<HistogramCountType> {40, 30, 5,  7},
      std::vector<HistogramCountType> {10, 20, 1,  1}};
    // clang-format on

    EXPECT_EQ(*pruned, expected_histogram);
  }

  {
    // Prune eight values from the last two bins with the boundary being on a single bin's value
    const auto pruned =
        std::static_pointer_cast<GenericHistogram<int32_t>>(histogram->pruned(8, PredicateCondition::LessThan, 60));
    const auto remaining_ratio = 1.0f - 8.0f / 15.0f;
    // clang-format off
    const auto expected_histogram = GenericHistogram<int32_t>{
      std::vector<int32_t>            { 1, 31, 60, 80},
      std::vector<int32_t>            {25, 50, 60, 99},
      std::vector<HistogramCountType> {40, 30, 5.0f*remaining_ratio, 10.0f*remaining_ratio},
      std::vector<HistogramCountType> {10, 20, 1,  1}};
    // clang-format on

    EXPECT_EQ(*pruned, expected_histogram);
  }

  {
    // Prune by equals predicate with value found in first bin
    const auto pruned =
        std::static_pointer_cast<GenericHistogram<int32_t>>(histogram->pruned(27, PredicateCondition::Equals, 17));
    // clang-format off
    const auto expected_histogram = GenericHistogram<int32_t>{
      std::vector<int32_t>            { 1, 31, 60, 80},
      std::vector<int32_t>            {25, 50, 60, 99},
      std::vector<HistogramCountType> {28, 20, 5*2.0f/3,  10*2.0f/3},
      std::vector<HistogramCountType> {10, 20, 1,  1}};
    // clang-format on

    EXPECT_EQ(*pruned, expected_histogram);
  }

  {
    // Prune by equals predicate with value not found anywhere
    const auto pruned =
        std::static_pointer_cast<GenericHistogram<int32_t>>(histogram->pruned(17, PredicateCondition::Equals, 61));

    // clang-format off
    const auto expected_histogram = GenericHistogram<int32_t>{
      std::vector<int32_t>            { 1, 31, 60, 80},
      std::vector<int32_t>            {25, 50, 60, 99},
      std::vector<HistogramCountType> {32, 24, 4,  8},
      std::vector<HistogramCountType> {10, 20, 1,  1}};
    // clang-format on

    EXPECT_EQ(*pruned, expected_histogram);
  }

  {
    // Prune by not equals predicate from first bin
    const auto pruned =
        std::static_pointer_cast<GenericHistogram<int32_t>>(histogram->pruned(3, PredicateCondition::NotEquals, 21));
    // clang-format off
    const auto expected_histogram = GenericHistogram<int32_t>{
      std::vector<int32_t>            { 1, 31, 60, 80},
      std::vector<int32_t>            {25, 50, 60, 99},
      std::vector<HistogramCountType> {37, 30, 5,  10},
      std::vector<HistogramCountType> {10, 20, 1,  1}};
    // clang-format on

    EXPECT_EQ(*pruned, expected_histogram);
  }

  {
    // Prune by not equals predicate not found in any bin
    const auto pruned =
        std::static_pointer_cast<GenericHistogram<int32_t>>(histogram->pruned(15, PredicateCondition::NotEquals, 28));

    EXPECT_EQ(*pruned, *histogram);
  }

  {
    // Prune by Between entirely contained in one bin
    const auto pruned = std::static_pointer_cast<GenericHistogram<int32_t>>(
        histogram->pruned(14, PredicateCondition::BetweenInclusive, 41, 50));
    // clang-format off
    const auto expected_histogram = GenericHistogram<int32_t>{
      std::vector<int32_t>            { 1, 31, 60, 80},
      std::vector<int32_t>            {25, 50, 60, 99},
      std::vector<HistogramCountType> {32, 27, 4,  8},
      std::vector<HistogramCountType> {10, 20, 1,  1}};
    // clang-format on

    EXPECT_EQ(*pruned, expected_histogram);
  }

  {
    // Prune by Between spanning three bins, covering half of the bins on both ends
    const auto pruned = std::static_pointer_cast<GenericHistogram<int32_t>>(
        histogram->pruned(24, PredicateCondition::BetweenExclusive, 41, 89));
    // clang-format off
    const auto expected_histogram = GenericHistogram<int32_t>{
      std::vector<int32_t>            { 1, 31, 60, 80},
      std::vector<int32_t>            {25, 50, 60, 99},
      std::vector<HistogramCountType> {24, 24, 5,  8},
      std::vector<HistogramCountType> {10, 20, 1,  1}};
    // clang-format on

    EXPECT_EQ(*pruned, expected_histogram);
  }

  {
    // Prune by Between spanning two bins, with the begin value being outside of the histogram and the end value in the
    // middle of the second bin
    const auto pruned = std::static_pointer_cast<GenericHistogram<int32_t>>(
        histogram->pruned(12, PredicateCondition::BetweenLowerExclusive, -10, 40));
    // clang-format off
    const auto expected_histogram = GenericHistogram<int32_t>{
      std::vector<int32_t>            { 1, 31,   60, 80},
      std::vector<int32_t>            {25, 50,   60, 99},
      std::vector<HistogramCountType> {40, 24,   3,  6},
      std::vector<HistogramCountType> {10, 20,   1,  1}};
    // clang-format on

    EXPECT_EQ(*pruned, expected_histogram);
  }

  {
    // Prune from the first and second bin. Prune more values than the bins contain.
    const auto pruned = std::static_pointer_cast<GenericHistogram<int32_t>>(
        histogram->pruned(100, PredicateCondition::GreaterThan, 40));
    // clang-format off
    const auto expected_histogram = GenericHistogram<int32_t>{
      std::vector<int32_t>            {31, 60, 80},
      std::vector<int32_t>            {50, 60, 99},
      std::vector<HistogramCountType> {15, 5,  10},
      std::vector<HistogramCountType> {20, 1,  1}};
    // clang-format on

    EXPECT_EQ(*pruned, expected_histogram);
  }
}

TEST_F(GenericHistogramTest, PrunedString) {
  // Same cardinalities as in PrunedInt, just with string values. Strings are chosen so that their numeric
  // representation is the same as the numbers used in PrunedInt, too.
  // clang-format off
  const auto histogram = std::make_shared<GenericHistogram<pmr_string>>(std::vector<pmr_string>        { "a", "bc", "ce", "cy"},  // NOLINT
                                                                        std::vector<pmr_string>        {"ax", "bv", "ce", "dq"},  // NOLINT
                                                                        std::vector<HistogramCountType>{40,   30,   5,    10},    // NOLINT
                                                                        std::vector<HistogramCountType>{10,   20,   1,    1},     // NOLINT
                                                                        StringHistogramDomain{'a', 'z', 2u});
  // clang-format on

  {
    // Prune half of the values from the first bin
    const auto pruned = std::static_pointer_cast<GenericHistogram<pmr_string>>(
        histogram->pruned(20, PredicateCondition::GreaterThanEquals, "ay"));

    // clang-format off
    const auto expected_histogram = GenericHistogram<pmr_string>{
      std::vector<pmr_string>        { "a", "bc", "ce", "cy"},
      std::vector<pmr_string>        {"ax", "bv", "ce", "dq"},
      std::vector<HistogramCountType>{20,   30,   5,    10},
      std::vector<HistogramCountType>{10,   20,   1,    1},
      StringHistogramDomain{'a', 'z', 2u}};
    // clang-format on

    EXPECT_EQ(*pruned, expected_histogram);
  }

  {
    // Prune all values from the first bin and half of the values from the second bin
    const auto pruned = std::static_pointer_cast<GenericHistogram<pmr_string>>(
        histogram->pruned(55, PredicateCondition::GreaterThan, "bl"));
    // clang-format off
    const auto expected_histogram = GenericHistogram<pmr_string>{
      std::vector<pmr_string>        {"bc", "ce", "cy"},
      std::vector<pmr_string>        {"bv", "ce", "dq"},
      std::vector<HistogramCountType>{15,   5,    10},
      std::vector<HistogramCountType>{20,   1,    1},
      StringHistogramDomain{'a', 'z', 2u}};
    // clang-format on

    EXPECT_EQ(*pruned, expected_histogram);
  }

  {
    // Prune by Between entirely contained in one bin
    const auto pruned = std::static_pointer_cast<GenericHistogram<pmr_string>>(
        histogram->pruned(14, PredicateCondition::BetweenInclusive, "bl", "bu"));
    // clang-format off
    const auto expected_histogram = GenericHistogram<pmr_string>{
      std::vector<pmr_string>        { "a", "bc", "ce", "cy"},
      std::vector<pmr_string>        {"ax", "bv", "ce", "dq"},
      std::vector<HistogramCountType>{32,   27,   4,    8},
      std::vector<HistogramCountType>{10,   20,   1,    1},
      StringHistogramDomain{'a', 'z', 2u}};
    // clang-format on

    EXPECT_EQ(*pruned, expected_histogram);
  }

  {
    // Prune by Between spanning three bins, covering half of the bins on both ends
    const auto pruned = std::static_pointer_cast<GenericHistogram<pmr_string>>(
        histogram->pruned(24, PredicateCondition::BetweenExclusive, "bm", "dg"));
    // clang-format off
    const auto expected_histogram = GenericHistogram<pmr_string>{
      std::vector<pmr_string>        { "a", "bc", "ce", "cy"},
      std::vector<pmr_string>        {"ax", "bv", "ce", "dq"},
      std::vector<HistogramCountType>{24,   24,   5,    8},
      std::vector<HistogramCountType>{10,   20,   1,    1},
      StringHistogramDomain{'a', 'z', 2u}};
    // clang-format on

    EXPECT_EQ(*pruned, expected_histogram);
  }

  {
    // Same as the last test, but this time, the search values are longer than the domain's common prefix.
    // Remember that string_to_number adds 1 for strings that are longer than the prefix.
    const auto pruned = std::static_pointer_cast<GenericHistogram<pmr_string>>(
        histogram->pruned(24, PredicateCondition::BetweenExclusive, "blaba", "dfqfqs"));
    // clang-format off
    const auto expected_histogram = GenericHistogram<pmr_string>{
      std::vector<pmr_string>        { "a", "bc", "ce", "cy"},
      std::vector<pmr_string>        {"ax", "bv", "ce", "dq"},
      std::vector<HistogramCountType>{24,   24,   5,    8},
      std::vector<HistogramCountType>{10,   20,   1,    1},
      StringHistogramDomain{'a', 'z', 2u}};
    // clang-format on

    EXPECT_EQ(*pruned, expected_histogram);
  }

  {
    // Same as the last test, but this time, the search values contain characters outside of the domain.
    // cX should be converted to ca. This way, the third bin should be included fully and the fourth bin
    // partially.
    const auto pruned = std::static_pointer_cast<GenericHistogram<pmr_string>>(
        histogram->pruned(15, PredicateCondition::BetweenExclusive, "cX", "dg"));
    // clang-format off
    const auto expected_histogram = GenericHistogram<pmr_string>{
      std::vector<pmr_string>        { "a", "bc", "ce", "cy"},
      std::vector<pmr_string>        {"ax", "bv", "ce", "dq"},
      std::vector<HistogramCountType>{32,   24,   5,    9},
      std::vector<HistogramCountType>{10,   20,   1,    1},
      StringHistogramDomain{'a', 'z', 2u}};
    // clang-format on

    EXPECT_EQ(*pruned, expected_histogram);
  }
}

TEST_F(GenericHistogramTest, SplitAtEmptyBinBounds) {
  // clang-format off
  const auto histogram = GenericHistogram<int32_t>(
          std::vector<int32_t>{},
          std::vector<int32_t>{},
          std::vector<HistogramCountType>{},
          std::vector<HistogramCountType>{});
  // clang-format on

  const auto expected_minima = std::vector<int32_t>{};
  const auto expected_maxima = std::vector<int32_t>{};
  const auto expected_heights = std::vector<HistogramCountType>{};
  const auto expected_distinct_counts = std::vector<HistogramCountType>{};

  const auto new_hist = histogram.split_at_bin_bounds(std::vector<std::pair<int32_t, int32_t>>{{}, {}});

  EXPECT_EQ(new_hist->bin_count(), expected_minima.size());

  for (auto bin_id = BinID{0}; bin_id < expected_minima.size(); bin_id++) {
    EXPECT_EQ(new_hist->bin_minimum(bin_id), expected_minima[bin_id]);
    EXPECT_EQ(new_hist->bin_maximum(bin_id), expected_maxima[bin_id]);
    EXPECT_FLOAT_EQ(new_hist->bin_height(bin_id), expected_heights[bin_id]);
    EXPECT_FLOAT_EQ(new_hist->bin_distinct_count(bin_id), expected_distinct_counts[bin_id]);
  }
}

TEST_F(GenericHistogramTest, SplitAtBinBounds) {
  // clang-format off
  const auto histogram = GenericHistogram<int32_t>(
          std::vector<int32_t>{1,  30, 60, 80},
          std::vector<int32_t>{25, 50, 75, 100},
          std::vector<HistogramCountType>{40, 30, 20, 10},
          std::vector<HistogramCountType>{10, 20, 15, 5});
  // clang-format on

  const auto expected_minima = std::vector<int32_t>{1, 10, 16, 30, 36, 60, 80};
  const auto expected_maxima = std::vector<int32_t>{9, 15, 25, 35, 50, 75, 100};
  const auto expected_heights = std::vector<HistogramCountType>{14.4f, 9.6f, 16.0f, 8.57143f, 21.42857f, 20.0f, 10};
  const auto expected_distinct_counts =
      std::vector<HistogramCountType>{3.6f, 2.4f, 4.0f, 5.7142859f, 14.285714f, 15, 5};

  const auto new_hist = histogram.split_at_bin_bounds(std::vector<std::pair<int32_t, int32_t>>{{10, 15}, {28, 35}});

  EXPECT_EQ(new_hist->bin_count(), expected_minima.size());

  for (auto bin_id = BinID{0}; bin_id < expected_minima.size(); bin_id++) {
    EXPECT_EQ(new_hist->bin_minimum(bin_id), expected_minima[bin_id]);
    EXPECT_EQ(new_hist->bin_maximum(bin_id), expected_maxima[bin_id]);
    EXPECT_FLOAT_EQ(new_hist->bin_height(bin_id), expected_heights[bin_id]);
    EXPECT_FLOAT_EQ(new_hist->bin_distinct_count(bin_id), expected_distinct_counts[bin_id]);
  }
}

TEST_F(GenericHistogramTest, SplitAtBinBoundsTwoHistograms) {
  // clang-format off
  const auto histogram_1 = GenericHistogram<int32_t>(
          std::vector<int32_t>{0,  5, 15, 20, 35, 45, 50},
          std::vector<int32_t>{4, 10, 18, 29, 40, 48, 51},

          // We only care about the bin edges in this test.
          std::vector<HistogramCountType>{1, 1, 1, 1, 1, 1, 1},
          std::vector<HistogramCountType>{1, 1, 1, 1, 1, 1, 1});

  const auto histogram_2 = GenericHistogram<int32_t>(
          std::vector<int32_t>{2, 12, 40, 45, 50},
          std::vector<int32_t>{7, 25, 42, 48, 52},

          // We only care about the bin edges in this test.
          std::vector<HistogramCountType>{1, 1, 1, 1, 1},
          std::vector<HistogramCountType>{1, 1, 1, 1, 1});

  // Even though the histograms are supposed to have the same bin edges, they do not exactly match.
  // The reason is that bins which do not contain any values are not created,
  // so some bins are missing in one histogram, and some are missing in the other.
  const auto histogram_1_expected_minima = std::vector<int32_t>{0, 2, 5,  8,     15,     20, 26, 35, 40,     45, 50};
  const auto histogram_2_expected_minima = std::vector<int32_t>{   2, 5,     12, 15, 19, 20,         40, 41, 45, 50, 52};  // NOLINT
  const auto histogram_1_expected_maxima = std::vector<int32_t>{1, 4, 7, 10,     18,     25, 29, 39, 40,     48, 51};
  const auto histogram_2_expected_maxima = std::vector<int32_t>{   4, 7,     14, 18, 19, 25,         40, 42, 48, 51, 52};  // NOLINT
  // clang-format on

  const auto new_histogram_1 = histogram_1.split_at_bin_bounds(histogram_2.bin_bounds());
  const auto new_histogram_2 = histogram_2.split_at_bin_bounds(histogram_1.bin_bounds());
  EXPECT_EQ(new_histogram_1->bin_count(), histogram_1_expected_minima.size());
  EXPECT_EQ(new_histogram_2->bin_count(), histogram_2_expected_minima.size());

  for (auto bin_id = BinID{0}; bin_id < histogram_1_expected_minima.size(); bin_id++) {
    EXPECT_EQ(new_histogram_1->bin_minimum(bin_id), histogram_1_expected_minima[bin_id]);
    EXPECT_EQ(new_histogram_1->bin_maximum(bin_id), histogram_1_expected_maxima[bin_id]);
  }

  for (auto bin_id = BinID{0}; bin_id < histogram_2_expected_minima.size(); bin_id++) {
    EXPECT_EQ(new_histogram_2->bin_minimum(bin_id), histogram_2_expected_minima[bin_id]);
    EXPECT_EQ(new_histogram_2->bin_maximum(bin_id), histogram_2_expected_maxima[bin_id]);
  }
}

TEST_F(GenericHistogramTest, ScaledWithSelectivity) {
  // clang-format off
  const auto histogram = GenericHistogram<int32_t>(
    std::vector<int32_t>{1,  30, 60, 80},
    std::vector<int32_t>{25, 50, 75, 100},
    std::vector<HistogramCountType>{40, 30, 20, 10},
    std::vector<HistogramCountType>{10, 20, 15, 5});
  // clang-format on

  const auto scaled_statistics_object_05 = histogram.scaled(0.5f);
  const auto scaled_histogram_05 = std::dynamic_pointer_cast<AbstractHistogram<int32_t>>(scaled_statistics_object_05);
  ASSERT_TRUE(scaled_histogram_05);
  EXPECT_FLOAT_EQ(scaled_histogram_05->bin_height(BinID{0}), 20.0f);
  EXPECT_FLOAT_EQ(scaled_histogram_05->bin_distinct_count(BinID{0}), 10.0f);
  EXPECT_FLOAT_EQ(scaled_histogram_05->bin_height(BinID{3}), 5.0f);
  EXPECT_FLOAT_EQ(scaled_histogram_05->bin_distinct_count(BinID{3}), 5.0f);

  const auto scaled_statistics_object_01 = histogram.scaled(0.1f);
  const auto scaled_histogram_01 = std::dynamic_pointer_cast<AbstractHistogram<int32_t>>(scaled_statistics_object_01);
  ASSERT_TRUE(scaled_histogram_01);
  EXPECT_FLOAT_EQ(scaled_histogram_01->bin_height(BinID{0}), 4.0f);
  EXPECT_FLOAT_EQ(scaled_histogram_01->bin_distinct_count(BinID{0}), 4.0f);
  EXPECT_FLOAT_EQ(scaled_histogram_01->bin_height(BinID{3}), 1.0f);
  EXPECT_FLOAT_EQ(scaled_histogram_01->bin_distinct_count(BinID{3}), 1.0f);

  const auto scaled_statistics_object_10 = histogram.scaled(10.0f);
  const auto scaled_histogram_10 = std::dynamic_pointer_cast<AbstractHistogram<int32_t>>(scaled_statistics_object_10);
  ASSERT_TRUE(scaled_histogram_10);
  EXPECT_FLOAT_EQ(scaled_histogram_10->bin_height(BinID{0}), 400.0f);
  EXPECT_FLOAT_EQ(scaled_histogram_10->bin_distinct_count(BinID{0}), 10.0f);
  EXPECT_FLOAT_EQ(scaled_histogram_10->bin_height(BinID{3}), 100.0f);
  EXPECT_FLOAT_EQ(scaled_histogram_10->bin_distinct_count(BinID{3}), 5.0f);
}

}  // namespace opossum
