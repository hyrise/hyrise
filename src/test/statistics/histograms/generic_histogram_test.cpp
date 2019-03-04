#include <limits>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "base_test.hpp"
#include "gtest/gtest.h"

#include "constant_mappings.hpp"
#include "statistics/histograms/generic_histogram.hpp"
#include "statistics/histograms/histogram_utils.hpp"
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
  return HistogramDomain<T>{}.next_value(v);
}

template <typename T>
T previous_value(T v) {
  return HistogramDomain<T>{}.previous_value(v);
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
            histogram->estimate_cardinality(predicate.predicate_condition, predicate.value, predicate.value2)
                .cardinality;

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

  EXPECT_EQ(histogram.estimate_cardinality(PredicateCondition::Equals, 0).type, EstimateType::MatchesNone);
  EXPECT_FLOAT_EQ(histogram.estimate_cardinality(PredicateCondition::Equals, 0).cardinality, 0.f);

  EXPECT_EQ(histogram.estimate_cardinality(PredicateCondition::Equals, 12).type, EstimateType::MatchesApproximately);
  EXPECT_FLOAT_EQ(histogram.estimate_cardinality(PredicateCondition::Equals, 12).cardinality, 1.f);

  EXPECT_EQ(histogram.estimate_cardinality(PredicateCondition::Equals, 1'234).type, EstimateType::MatchesNone);
  EXPECT_FLOAT_EQ(histogram.estimate_cardinality(PredicateCondition::Equals, 1'234).cardinality, 0.f);

  EXPECT_EQ(histogram.estimate_cardinality(PredicateCondition::Equals, 123'456).type,
            EstimateType::MatchesApproximately);
  EXPECT_FLOAT_EQ(histogram.estimate_cardinality(PredicateCondition::Equals, 123'456).cardinality, 2.5f);

  EXPECT_EQ(histogram.estimate_cardinality(PredicateCondition::Equals, 1'000'000).type, EstimateType::MatchesNone);
  EXPECT_FLOAT_EQ(histogram.estimate_cardinality(PredicateCondition::Equals, 1'000'000).cardinality, 0.f);
}

TEST_F(GenericHistogramTest, EstimateCardinalityAndPruningBasicFloat) {
  const auto histogram = GenericHistogram<float>{{0.5f, 2.5f, 3.6f}, {2.2f, 3.3f, 6.1f}, {4, 6, 4}, {4, 3, 3}};

  EXPECT_EQ(histogram.estimate_cardinality(PredicateCondition::Equals, 0.4f).type, EstimateType::MatchesNone);
  EXPECT_FLOAT_EQ(histogram.estimate_cardinality(PredicateCondition::Equals, 0.4f).cardinality, 0.f);

  EXPECT_EQ(histogram.estimate_cardinality(PredicateCondition::Equals, 0.5f).type, EstimateType::MatchesApproximately);
  EXPECT_FLOAT_EQ(histogram.estimate_cardinality(PredicateCondition::Equals, 0.5f).cardinality, 4 / 4.f);

  EXPECT_EQ(histogram.estimate_cardinality(PredicateCondition::Equals, 1.1f).type, EstimateType::MatchesApproximately);
  EXPECT_FLOAT_EQ(histogram.estimate_cardinality(PredicateCondition::Equals, 1.1f).cardinality, 4 / 4.f);

  EXPECT_EQ(histogram.estimate_cardinality(PredicateCondition::Equals, 1.3f).type, EstimateType::MatchesApproximately);
  EXPECT_FLOAT_EQ(histogram.estimate_cardinality(PredicateCondition::Equals, 1.3f).cardinality, 4 / 4.f);

  EXPECT_EQ(histogram.estimate_cardinality(PredicateCondition::Equals, 2.2f).type, EstimateType::MatchesApproximately);
  EXPECT_FLOAT_EQ(histogram.estimate_cardinality(PredicateCondition::Equals, 2.2f).cardinality, 4 / 4.f);

  EXPECT_EQ(histogram.estimate_cardinality(PredicateCondition::Equals, 2.3f).type, EstimateType::MatchesNone);
  EXPECT_FLOAT_EQ(histogram.estimate_cardinality(PredicateCondition::Equals, 2.3f).cardinality, 0.f);

  EXPECT_EQ(histogram.estimate_cardinality(PredicateCondition::Equals, 2.5f).type, EstimateType::MatchesApproximately);
  EXPECT_FLOAT_EQ(histogram.estimate_cardinality(PredicateCondition::Equals, 2.5f).cardinality, 6 / 3.f);

  EXPECT_EQ(histogram.estimate_cardinality(PredicateCondition::Equals, 2.9f).type, EstimateType::MatchesApproximately);
  EXPECT_FLOAT_EQ(histogram.estimate_cardinality(PredicateCondition::Equals, 2.9f).cardinality, 6 / 3.f);

  EXPECT_EQ(histogram.estimate_cardinality(PredicateCondition::Equals, 3.3f).type, EstimateType::MatchesApproximately);
  EXPECT_FLOAT_EQ(histogram.estimate_cardinality(PredicateCondition::Equals, 3.3f).cardinality, 6 / 3.f);

  EXPECT_EQ(histogram.estimate_cardinality(PredicateCondition::Equals, 3.5f).type, EstimateType::MatchesNone);
  EXPECT_FLOAT_EQ(histogram.estimate_cardinality(PredicateCondition::Equals, 3.5f).cardinality, 0.f);

  EXPECT_EQ(histogram.estimate_cardinality(PredicateCondition::Equals, 3.6f).type, EstimateType::MatchesApproximately);
  EXPECT_FLOAT_EQ(histogram.estimate_cardinality(PredicateCondition::Equals, 3.6f).cardinality, 4 / 3.f);

  EXPECT_EQ(histogram.estimate_cardinality(PredicateCondition::Equals, 3.9f).type, EstimateType::MatchesApproximately);
  EXPECT_FLOAT_EQ(histogram.estimate_cardinality(PredicateCondition::Equals, 3.9f).cardinality, 4 / 3.f);

  EXPECT_EQ(histogram.estimate_cardinality(PredicateCondition::Equals, 6.1f).type, EstimateType::MatchesApproximately);
  EXPECT_FLOAT_EQ(histogram.estimate_cardinality(PredicateCondition::Equals, 6.1f).cardinality, 4 / 3.f);

  EXPECT_EQ(histogram.estimate_cardinality(PredicateCondition::Equals, 6.2f).type, EstimateType::MatchesNone);
  EXPECT_FLOAT_EQ(histogram.estimate_cardinality(PredicateCondition::Equals, 6.2f).cardinality, 0.f);
}

TEST_F(GenericHistogramTest, EstimateCardinalityAndPruningBasicString) {
  const auto histogram = GenericHistogram<pmr_string>{
      {"aa", "bla", "uuu", "yyy"}, {"birne", "ttt", "xxx", "zzz"}, {3, 4, 4, 4}, {3, 3, 3, 2}, StringHistogramDomain{}

  };

  EXPECT_EQ(histogram.estimate_cardinality(PredicateCondition::Equals, "a").type, EstimateType::MatchesNone);
  EXPECT_FLOAT_EQ(histogram.estimate_cardinality(PredicateCondition::Equals, "a").cardinality, 0.f);

  EXPECT_EQ(histogram.estimate_cardinality(PredicateCondition::Equals, "aa").type, EstimateType::MatchesApproximately);
  EXPECT_FLOAT_EQ(histogram.estimate_cardinality(PredicateCondition::Equals, "aa").cardinality, 3 / 3.f);

  EXPECT_EQ(histogram.estimate_cardinality(PredicateCondition::Equals, "ab").type, EstimateType::MatchesApproximately);
  EXPECT_FLOAT_EQ(histogram.estimate_cardinality(PredicateCondition::Equals, "ab").cardinality, 3 / 3.f);

  EXPECT_EQ(histogram.estimate_cardinality(PredicateCondition::Equals, "b").type, EstimateType::MatchesApproximately);
  EXPECT_FLOAT_EQ(histogram.estimate_cardinality(PredicateCondition::Equals, "b").cardinality, 3 / 3.f);

  EXPECT_EQ(histogram.estimate_cardinality(PredicateCondition::Equals, "birne").type,
            EstimateType::MatchesApproximately);
  EXPECT_FLOAT_EQ(histogram.estimate_cardinality(PredicateCondition::Equals, "birne").cardinality, 3 / 3.f);

  EXPECT_EQ(histogram.estimate_cardinality(PredicateCondition::Equals, "biscuit").type, EstimateType::MatchesNone);
  EXPECT_FLOAT_EQ(histogram.estimate_cardinality(PredicateCondition::Equals, "biscuit").cardinality, 0.f);

  EXPECT_EQ(histogram.estimate_cardinality(PredicateCondition::Equals, "bla").type, EstimateType::MatchesApproximately);
  EXPECT_FLOAT_EQ(histogram.estimate_cardinality(PredicateCondition::Equals, "bla").cardinality, 4 / 3.f);

  EXPECT_EQ(histogram.estimate_cardinality(PredicateCondition::Equals, "blubb").type,
            EstimateType::MatchesApproximately);
  EXPECT_FLOAT_EQ(histogram.estimate_cardinality(PredicateCondition::Equals, "blubb").cardinality, 4 / 3.f);

  EXPECT_EQ(histogram.estimate_cardinality(PredicateCondition::Equals, "bums").type,
            EstimateType::MatchesApproximately);
  EXPECT_FLOAT_EQ(histogram.estimate_cardinality(PredicateCondition::Equals, "bums").cardinality, 4 / 3.f);

  EXPECT_EQ(histogram.estimate_cardinality(PredicateCondition::Equals, "ttt").type, EstimateType::MatchesApproximately);
  EXPECT_FLOAT_EQ(histogram.estimate_cardinality(PredicateCondition::Equals, "ttt").cardinality, 4 / 3.f);

  EXPECT_EQ(histogram.estimate_cardinality(PredicateCondition::Equals, "turkey").type, EstimateType::MatchesNone);
  EXPECT_FLOAT_EQ(histogram.estimate_cardinality(PredicateCondition::Equals, "turkey").cardinality, 0.f);

  EXPECT_EQ(histogram.estimate_cardinality(PredicateCondition::Equals, "uuu").type, EstimateType::MatchesApproximately);
  EXPECT_FLOAT_EQ(histogram.estimate_cardinality(PredicateCondition::Equals, "uuu").cardinality, 4 / 3.f);

  EXPECT_EQ(histogram.estimate_cardinality(PredicateCondition::Equals, "vvv").type, EstimateType::MatchesApproximately);
  EXPECT_FLOAT_EQ(histogram.estimate_cardinality(PredicateCondition::Equals, "vvv").cardinality, 4 / 3.f);

  EXPECT_EQ(histogram.estimate_cardinality(PredicateCondition::Equals, "www").type, EstimateType::MatchesApproximately);
  EXPECT_FLOAT_EQ(histogram.estimate_cardinality(PredicateCondition::Equals, "www").cardinality, 4 / 3.f);

  EXPECT_EQ(histogram.estimate_cardinality(PredicateCondition::Equals, "xxx").type, EstimateType::MatchesApproximately);
  EXPECT_FLOAT_EQ(histogram.estimate_cardinality(PredicateCondition::Equals, "xxx").cardinality, 4 / 3.f);

  EXPECT_EQ(histogram.estimate_cardinality(PredicateCondition::Equals, "yyy").type, EstimateType::MatchesApproximately);
  EXPECT_FLOAT_EQ(histogram.estimate_cardinality(PredicateCondition::Equals, "yyy").cardinality, 4 / 2.f);

  EXPECT_EQ(histogram.estimate_cardinality(PredicateCondition::Equals, "zzz").type, EstimateType::MatchesApproximately);
  EXPECT_FLOAT_EQ(histogram.estimate_cardinality(PredicateCondition::Equals, "zzz").cardinality, 4 / 2.f);

  EXPECT_EQ(histogram.estimate_cardinality(PredicateCondition::Equals, "zzzzzz").type, EstimateType::MatchesNone);
  EXPECT_FLOAT_EQ(histogram.estimate_cardinality(PredicateCondition::Equals, "zzzzzz").cardinality, 0.f);
}

TEST_F(GenericHistogramTest, StringPruning) {
  const auto histogram = GenericHistogram<pmr_string>{{"aa", "bla", "uuu", "yyy"},
                                                       {"birne", "ttt", "xxx", "zzz"},
                                                       {3, 4, 4, 4},
                                                       {3, 3, 3, 2},
                                                       StringHistogramDomain{'a', 'z', 3u}};

  // These values are smaller than values in bin 0.
  EXPECT_EQ(histogram.estimate_cardinality(PredicateCondition::Equals, "").type, EstimateType::MatchesNone);
  EXPECT_EQ(histogram.estimate_cardinality(PredicateCondition::Equals, "a").type, EstimateType::MatchesNone);

  // These values fall within bin 0.
  EXPECT_EQ(histogram.estimate_cardinality(PredicateCondition::Equals, "aa").type, EstimateType::MatchesApproximately);
  EXPECT_EQ(histogram.estimate_cardinality(PredicateCondition::Equals, "aaa").type, EstimateType::MatchesApproximately);
  EXPECT_EQ(histogram.estimate_cardinality(PredicateCondition::Equals, "b").type, EstimateType::MatchesApproximately);
  EXPECT_EQ(histogram.estimate_cardinality(PredicateCondition::Equals, "bir").type, EstimateType::MatchesApproximately);
  EXPECT_EQ(histogram.estimate_cardinality(PredicateCondition::Equals, "bira").type,
            EstimateType::MatchesApproximately);
  EXPECT_EQ(histogram.estimate_cardinality(PredicateCondition::Equals, "birne").type,
            EstimateType::MatchesApproximately);

  // These values are between bin 0 and 1.
  EXPECT_EQ(histogram.estimate_cardinality(PredicateCondition::Equals, "birnea").type, EstimateType::MatchesNone);
  EXPECT_EQ(histogram.estimate_cardinality(PredicateCondition::Equals, "bis").type, EstimateType::MatchesNone);
  EXPECT_EQ(histogram.estimate_cardinality(PredicateCondition::Equals, "biscuit").type, EstimateType::MatchesNone);
  EXPECT_EQ(histogram.estimate_cardinality(PredicateCondition::Equals, "bja").type, EstimateType::MatchesNone);
  EXPECT_EQ(histogram.estimate_cardinality(PredicateCondition::Equals, "bk").type, EstimateType::MatchesNone);
  EXPECT_EQ(histogram.estimate_cardinality(PredicateCondition::Equals, "bkz").type, EstimateType::MatchesNone);
  EXPECT_EQ(histogram.estimate_cardinality(PredicateCondition::Equals, "bl").type, EstimateType::MatchesNone);

  // These values fall within bin 1.
  EXPECT_EQ(histogram.estimate_cardinality(PredicateCondition::Equals, "bla").type, EstimateType::MatchesApproximately);
  EXPECT_EQ(histogram.estimate_cardinality(PredicateCondition::Equals, "c").type, EstimateType::MatchesApproximately);
  EXPECT_EQ(histogram.estimate_cardinality(PredicateCondition::Equals, "mmopasdasdasd").type,
            EstimateType::MatchesApproximately);
  EXPECT_EQ(histogram.estimate_cardinality(PredicateCondition::Equals, "s").type, EstimateType::MatchesApproximately);
  EXPECT_EQ(histogram.estimate_cardinality(PredicateCondition::Equals, "t").type, EstimateType::MatchesApproximately);
  EXPECT_EQ(histogram.estimate_cardinality(PredicateCondition::Equals, "tt").type, EstimateType::MatchesApproximately);
  EXPECT_EQ(histogram.estimate_cardinality(PredicateCondition::Equals, "ttt").type, EstimateType::MatchesApproximately);

  // These values are between bin 1 and 2.
  EXPECT_EQ(histogram.estimate_cardinality(PredicateCondition::Equals, "ttta").type, EstimateType::MatchesNone);
  EXPECT_EQ(histogram.estimate_cardinality(PredicateCondition::Equals, "tttzzzzz").type, EstimateType::MatchesNone);
  EXPECT_EQ(histogram.estimate_cardinality(PredicateCondition::Equals, "turkey").type, EstimateType::MatchesNone);
  EXPECT_EQ(histogram.estimate_cardinality(PredicateCondition::Equals, "uut").type, EstimateType::MatchesNone);
  EXPECT_EQ(histogram.estimate_cardinality(PredicateCondition::Equals, "uutzzzzz").type, EstimateType::MatchesNone);

  // These values fall within bin 2.
  EXPECT_EQ(histogram.estimate_cardinality(PredicateCondition::Equals, "uuu").type, EstimateType::MatchesApproximately);
  EXPECT_EQ(histogram.estimate_cardinality(PredicateCondition::Equals, "uuuzzz").type,
            EstimateType::MatchesApproximately);
  EXPECT_EQ(histogram.estimate_cardinality(PredicateCondition::Equals, "uv").type, EstimateType::MatchesApproximately);
  EXPECT_EQ(histogram.estimate_cardinality(PredicateCondition::Equals, "uvz").type, EstimateType::MatchesApproximately);
  EXPECT_EQ(histogram.estimate_cardinality(PredicateCondition::Equals, "v").type, EstimateType::MatchesApproximately);
  EXPECT_EQ(histogram.estimate_cardinality(PredicateCondition::Equals, "w").type, EstimateType::MatchesApproximately);
  EXPECT_EQ(histogram.estimate_cardinality(PredicateCondition::Equals, "wzzzzzzzzz").type,
            EstimateType::MatchesApproximately);
  EXPECT_EQ(histogram.estimate_cardinality(PredicateCondition::Equals, "x").type, EstimateType::MatchesApproximately);
  EXPECT_EQ(histogram.estimate_cardinality(PredicateCondition::Equals, "xxw").type, EstimateType::MatchesApproximately);
  EXPECT_EQ(histogram.estimate_cardinality(PredicateCondition::Equals, "xxx").type, EstimateType::MatchesApproximately);

  // These values are between bin 2 and 3.
  EXPECT_EQ(histogram.estimate_cardinality(PredicateCondition::Equals, "xxxa").type, EstimateType::MatchesNone);
  EXPECT_EQ(histogram.estimate_cardinality(PredicateCondition::Equals, "xxxzzzzzz").type, EstimateType::MatchesNone);
  EXPECT_EQ(histogram.estimate_cardinality(PredicateCondition::Equals, "xy").type, EstimateType::MatchesNone);
  EXPECT_EQ(histogram.estimate_cardinality(PredicateCondition::Equals, "xyzz").type, EstimateType::MatchesNone);
  EXPECT_EQ(histogram.estimate_cardinality(PredicateCondition::Equals, "y").type, EstimateType::MatchesNone);
  EXPECT_EQ(histogram.estimate_cardinality(PredicateCondition::Equals, "yyx").type, EstimateType::MatchesNone);
  EXPECT_EQ(histogram.estimate_cardinality(PredicateCondition::Equals, "yyxzzzzz").type, EstimateType::MatchesNone);

  // These values fall within bin 3.
  EXPECT_EQ(histogram.estimate_cardinality(PredicateCondition::Equals, "yyy").type, EstimateType::MatchesApproximately);
  EXPECT_EQ(histogram.estimate_cardinality(PredicateCondition::Equals, "yyyzzzzz").type,
            EstimateType::MatchesApproximately);
  EXPECT_EQ(histogram.estimate_cardinality(PredicateCondition::Equals, "yz").type, EstimateType::MatchesApproximately);
  EXPECT_EQ(histogram.estimate_cardinality(PredicateCondition::Equals, "z").type, EstimateType::MatchesApproximately);
  EXPECT_EQ(histogram.estimate_cardinality(PredicateCondition::Equals, "zzz").type, EstimateType::MatchesApproximately);

  // These values are greater than the upper bound of the histogram.
  EXPECT_EQ(histogram.estimate_cardinality(PredicateCondition::Equals, "zzza").type, EstimateType::MatchesNone);
  EXPECT_EQ(histogram.estimate_cardinality(PredicateCondition::Equals, "zzzzzzzzz").type, EstimateType::MatchesNone);
}

TEST_F(GenericHistogramTest, FloatLessThan) {
  const auto histogram = GenericHistogram<float>{{0.5f, 2.5f, 3.6f}, {2.2f, 3.3f, 6.1f}, {4, 6, 4}, {4, 3, 3}};

  EXPECT_EQ(histogram.estimate_cardinality(PredicateCondition::LessThan, 0.5f).type, EstimateType::MatchesNone);
  EXPECT_FLOAT_EQ(histogram.estimate_cardinality(PredicateCondition::LessThan, 0.5f).cardinality, 0.f);

  EXPECT_EQ(histogram.estimate_cardinality(PredicateCondition::LessThan, 1.0f).type,
            EstimateType::MatchesApproximately);
  EXPECT_FLOAT_EQ(histogram.estimate_cardinality(PredicateCondition::LessThan, 1.0f).cardinality,
                  (1.0f - 0.5f) / std::nextafter(2.2f - 0.5f, std::numeric_limits<float>::infinity()) * 4);

  EXPECT_EQ(histogram.estimate_cardinality(PredicateCondition::LessThan, 1.7f).type,
            EstimateType::MatchesApproximately);
  EXPECT_FLOAT_EQ(histogram.estimate_cardinality(PredicateCondition::LessThan, 1.7f).cardinality,
                  (1.7f - 0.5f) / std::nextafter(2.2f - 0.5f, std::numeric_limits<float>::infinity()) * 4);

  EXPECT_EQ(histogram
                .estimate_cardinality(PredicateCondition::LessThan,
                                      std::nextafter(2.2f, std::numeric_limits<float>::infinity()))
                .type,
            EstimateType::MatchesExactly);
  EXPECT_FLOAT_EQ(histogram
                      .estimate_cardinality(PredicateCondition::LessThan,
                                            std::nextafter(2.2f, std::numeric_limits<float>::infinity()))
                      .cardinality,
                  4.f);

  EXPECT_EQ(histogram.estimate_cardinality(PredicateCondition::LessThan, 2.5f).type, EstimateType::MatchesExactly);
  EXPECT_FLOAT_EQ(histogram.estimate_cardinality(PredicateCondition::LessThan, 2.5f).cardinality, 4.f);

  EXPECT_EQ(histogram.estimate_cardinality(PredicateCondition::LessThan, 3.0f).type,
            EstimateType::MatchesApproximately);
  EXPECT_FLOAT_EQ(histogram.estimate_cardinality(PredicateCondition::LessThan, 3.0f).cardinality,
                  4.f + (3.0f - 2.5f) / std::nextafter(3.3f - 2.5f, std::numeric_limits<float>::infinity()) * 6);

  EXPECT_EQ(histogram.estimate_cardinality(PredicateCondition::LessThan, 3.3f).type,
            EstimateType::MatchesApproximately);
  EXPECT_FLOAT_EQ(histogram.estimate_cardinality(PredicateCondition::LessThan, 3.3f).cardinality,
                  4.f + (3.3f - 2.5f) / std::nextafter(3.3f - 2.5f, std::numeric_limits<float>::infinity()) * 6);

  EXPECT_EQ(histogram
                .estimate_cardinality(PredicateCondition::LessThan,
                                      std::nextafter(3.3f, std::numeric_limits<float>::infinity()))
                .type,
            EstimateType::MatchesExactly);
  EXPECT_FLOAT_EQ(histogram
                      .estimate_cardinality(PredicateCondition::LessThan,
                                            std::nextafter(3.3f, std::numeric_limits<float>::infinity()))
                      .cardinality,
                  4.f + 6.f);

  EXPECT_EQ(histogram.estimate_cardinality(PredicateCondition::LessThan, 3.6f).type, EstimateType::MatchesExactly);
  EXPECT_FLOAT_EQ(histogram.estimate_cardinality(PredicateCondition::LessThan, 3.6f).cardinality, 4.f + 6.f);

  EXPECT_EQ(histogram.estimate_cardinality(PredicateCondition::LessThan, 3.9f).type,
            EstimateType::MatchesApproximately);
  EXPECT_FLOAT_EQ(histogram.estimate_cardinality(PredicateCondition::LessThan, 3.9f).cardinality,
                  4.f + 6.f + (3.9f - 3.6f) / std::nextafter(6.1f - 3.6f, std::numeric_limits<float>::infinity()) * 4);

  EXPECT_EQ(histogram.estimate_cardinality(PredicateCondition::LessThan, 5.9f).type,
            EstimateType::MatchesApproximately);
  EXPECT_FLOAT_EQ(histogram.estimate_cardinality(PredicateCondition::LessThan, 5.9f).cardinality,
                  4.f + 6.f + (5.9f - 3.6f) / std::nextafter(6.1f - 3.6f, std::numeric_limits<float>::infinity()) * 4);

  EXPECT_EQ(histogram
                .estimate_cardinality(PredicateCondition::LessThan,
                                      std::nextafter(6.1f, std::numeric_limits<float>::infinity()))
                .type,
            EstimateType::MatchesAll);
  EXPECT_FLOAT_EQ(histogram
                      .estimate_cardinality(PredicateCondition::LessThan,
                                            std::nextafter(6.1f, std::numeric_limits<float>::infinity()))
                      .cardinality,
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

  EXPECT_EQ(histogram.estimate_cardinality(PredicateCondition::LessThan, "aaaa").type, EstimateType::MatchesNone);
  EXPECT_FLOAT_EQ(histogram.estimate_cardinality(PredicateCondition::LessThan, "aaaa").cardinality, 0.f);

  EXPECT_EQ(histogram.estimate_cardinality(PredicateCondition::LessThan, "abcd").type, EstimateType::MatchesNone);
  EXPECT_FLOAT_EQ(histogram.estimate_cardinality(PredicateCondition::LessThan, "abcd").cardinality, 0.f);

  EXPECT_EQ(histogram.estimate_cardinality(PredicateCondition::LessThan, "abce").type,
            EstimateType::MatchesApproximately);
  EXPECT_FLOAT_EQ(histogram.estimate_cardinality(PredicateCondition::LessThan, "abce").cardinality,
                  1 / bin_1_width * bin_1_count);

  EXPECT_EQ(histogram.estimate_cardinality(PredicateCondition::LessThan, "abcf").type,
            EstimateType::MatchesApproximately);
  EXPECT_FLOAT_EQ(histogram.estimate_cardinality(PredicateCondition::LessThan, "abcf").cardinality,
                  2 / bin_1_width * bin_1_count);

  EXPECT_EQ(histogram.estimate_cardinality(PredicateCondition::LessThan, "abcf").type,
            EstimateType::MatchesApproximately);
  EXPECT_FLOAT_EQ(
      histogram.estimate_cardinality(PredicateCondition::LessThan, "cccc").cardinality,
      (2 * (ipow(26, 3) + ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1 + 2 * (ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) +
       1 + 2 * (ipow(26, 1) + ipow(26, 0)) + 1 + 2 * (ipow(26, 0)) + 1 - bin_1_lower) /
          bin_1_width * bin_1_count);

  EXPECT_EQ(histogram.estimate_cardinality(PredicateCondition::LessThan, "dddd").type,
            EstimateType::MatchesApproximately);
  EXPECT_FLOAT_EQ(
      histogram.estimate_cardinality(PredicateCondition::LessThan, "dddd").cardinality,
      (3 * (ipow(26, 3) + ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1 + 3 * (ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) +
       1 + 3 * (ipow(26, 1) + ipow(26, 0)) + 1 + 3 * (ipow(26, 0)) + 1 - bin_1_lower) /
          bin_1_width * bin_1_count);

  EXPECT_EQ(histogram.estimate_cardinality(PredicateCondition::LessThan, "efgg").type,
            EstimateType::MatchesApproximately);
  EXPECT_FLOAT_EQ(histogram.estimate_cardinality(PredicateCondition::LessThan, "efgg").cardinality,
                  (bin_1_width - 2) / bin_1_width * bin_1_count);

  EXPECT_EQ(histogram.estimate_cardinality(PredicateCondition::LessThan, "efgh").type,
            EstimateType::MatchesApproximately);
  EXPECT_FLOAT_EQ(histogram.estimate_cardinality(PredicateCondition::LessThan, "efgh").cardinality,
                  (bin_1_width - 1) / bin_1_width * bin_1_count);

  EXPECT_EQ(histogram.estimate_cardinality(PredicateCondition::LessThan, "efgi").type, EstimateType::MatchesExactly);
  EXPECT_FLOAT_EQ(histogram.estimate_cardinality(PredicateCondition::LessThan, "efgi").cardinality, bin_1_count);

  EXPECT_EQ(histogram.estimate_cardinality(PredicateCondition::LessThan, "ijkl").type, EstimateType::MatchesExactly);
  EXPECT_FLOAT_EQ(histogram.estimate_cardinality(PredicateCondition::LessThan, "ijkl").cardinality, bin_1_count);

  EXPECT_EQ(histogram.estimate_cardinality(PredicateCondition::LessThan, "ijkm").type,
            EstimateType::MatchesApproximately);
  EXPECT_FLOAT_EQ(histogram.estimate_cardinality(PredicateCondition::LessThan, "ijkm").cardinality,
                  1 / bin_2_width * bin_2_count + bin_1_count);

  EXPECT_EQ(histogram.estimate_cardinality(PredicateCondition::LessThan, "ijkn").type,
            EstimateType::MatchesApproximately);
  EXPECT_FLOAT_EQ(histogram.estimate_cardinality(PredicateCondition::LessThan, "ijkn").cardinality,
                  2 / bin_2_width * bin_2_count + bin_1_count);

  EXPECT_EQ(histogram.estimate_cardinality(PredicateCondition::LessThan, "jjjj").type,
            EstimateType::MatchesApproximately);
  EXPECT_FLOAT_EQ(
      histogram.estimate_cardinality(PredicateCondition::LessThan, "jjjj").cardinality,
      (9 * (ipow(26, 3) + ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1 + 9 * (ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) +
       1 + 9 * (ipow(26, 1) + ipow(26, 0)) + 1 + 9 * (ipow(26, 0)) + 1 - bin_2_lower) /
              bin_2_width * bin_2_count +
          bin_1_count);

  EXPECT_EQ(histogram.estimate_cardinality(PredicateCondition::LessThan, "kkkk").type,
            EstimateType::MatchesApproximately);
  EXPECT_FLOAT_EQ(histogram.estimate_cardinality(PredicateCondition::LessThan, "kkkk").cardinality,
                  (10 * (ipow(26, 3) + ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1 +
                   10 * (ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1 + 10 * (ipow(26, 1) + ipow(26, 0)) + 1 +
                   10 * (ipow(26, 0)) + 1 - bin_2_lower) /
                          bin_2_width * bin_2_count +
                      bin_1_count);

  EXPECT_EQ(histogram.estimate_cardinality(PredicateCondition::LessThan, "lzzz").type,
            EstimateType::MatchesApproximately);
  EXPECT_FLOAT_EQ(histogram.estimate_cardinality(PredicateCondition::LessThan, "lzzz").cardinality,
                  (11 * (ipow(26, 3) + ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1 +
                   25 * (ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1 + 25 * (ipow(26, 1) + ipow(26, 0)) + 1 +
                   25 * (ipow(26, 0)) + 1 - bin_2_lower) /
                          bin_2_width * bin_2_count +
                      bin_1_count);

  EXPECT_EQ(histogram.estimate_cardinality(PredicateCondition::LessThan, "mnoo").type,
            EstimateType::MatchesApproximately);
  EXPECT_FLOAT_EQ(histogram.estimate_cardinality(PredicateCondition::LessThan, "mnoo").cardinality,
                  (bin_2_width - 2) / bin_2_width * bin_2_count + bin_1_count);

  EXPECT_EQ(histogram.estimate_cardinality(PredicateCondition::LessThan, "mnop").type,
            EstimateType::MatchesApproximately);
  EXPECT_FLOAT_EQ(histogram.estimate_cardinality(PredicateCondition::LessThan, "mnop").cardinality,
                  (bin_2_width - 1) / bin_2_width * bin_2_count + bin_1_count);

  EXPECT_EQ(histogram.estimate_cardinality(PredicateCondition::LessThan, "mnoq").type, EstimateType::MatchesExactly);
  EXPECT_FLOAT_EQ(histogram.estimate_cardinality(PredicateCondition::LessThan, "mnoq").cardinality,
                  bin_1_count + bin_2_count);

  EXPECT_EQ(histogram.estimate_cardinality(PredicateCondition::LessThan, "oopp").type, EstimateType::MatchesExactly);
  EXPECT_FLOAT_EQ(histogram.estimate_cardinality(PredicateCondition::LessThan, "oopp").cardinality,
                  bin_1_count + bin_2_count);

  EXPECT_EQ(histogram.estimate_cardinality(PredicateCondition::LessThan, "oopq").type,
            EstimateType::MatchesApproximately);
  EXPECT_FLOAT_EQ(histogram.estimate_cardinality(PredicateCondition::LessThan, "oopq").cardinality,
                  1 / bin_3_width * bin_3_count + bin_1_count + bin_2_count);

  EXPECT_EQ(histogram.estimate_cardinality(PredicateCondition::LessThan, "oopr").type,
            EstimateType::MatchesApproximately);
  EXPECT_FLOAT_EQ(histogram.estimate_cardinality(PredicateCondition::LessThan, "oopr").cardinality,
                  2 / bin_3_width * bin_3_count + bin_1_count + bin_2_count);

  EXPECT_EQ(histogram.estimate_cardinality(PredicateCondition::LessThan, "pppp").type,
            EstimateType::MatchesApproximately);
  EXPECT_FLOAT_EQ(histogram.estimate_cardinality(PredicateCondition::LessThan, "pppp").cardinality,
                  (15 * (ipow(26, 3) + ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1 +
                   15 * (ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1 + 15 * (ipow(26, 1) + ipow(26, 0)) + 1 +
                   15 * (ipow(26, 0)) + 1 - bin_3_lower) /
                          bin_3_width * bin_3_count +
                      bin_1_count + bin_2_count);

  EXPECT_EQ(histogram.estimate_cardinality(PredicateCondition::LessThan, "qqqq").type,
            EstimateType::MatchesApproximately);
  EXPECT_FLOAT_EQ(histogram.estimate_cardinality(PredicateCondition::LessThan, "qqqq").cardinality,
                  (16 * (ipow(26, 3) + ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1 +
                   16 * (ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1 + 16 * (ipow(26, 1) + ipow(26, 0)) + 1 +
                   16 * (ipow(26, 0)) + 1 - bin_3_lower) /
                          bin_3_width * bin_3_count +
                      bin_1_count + bin_2_count);

  EXPECT_EQ(histogram.estimate_cardinality(PredicateCondition::LessThan, "qllo").type,
            EstimateType::MatchesApproximately);
  EXPECT_FLOAT_EQ(histogram.estimate_cardinality(PredicateCondition::LessThan, "qllo").cardinality,
                  (16 * (ipow(26, 3) + ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1 +
                   11 * (ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1 + 11 * (ipow(26, 1) + ipow(26, 0)) + 1 +
                   14 * (ipow(26, 0)) + 1 - bin_3_lower) /
                          bin_3_width * bin_3_count +
                      bin_1_count + bin_2_count);

  EXPECT_EQ(histogram.estimate_cardinality(PredicateCondition::LessThan, "qrss").type,
            EstimateType::MatchesApproximately);
  EXPECT_FLOAT_EQ(histogram.estimate_cardinality(PredicateCondition::LessThan, "qrss").cardinality,
                  (bin_3_width - 2) / bin_3_width * bin_3_count + bin_1_count + bin_2_count);

  EXPECT_EQ(histogram.estimate_cardinality(PredicateCondition::LessThan, "qrst").type,
            EstimateType::MatchesApproximately);
  EXPECT_FLOAT_EQ(histogram.estimate_cardinality(PredicateCondition::LessThan, "qrst").cardinality,
                  (bin_3_width - 1) / bin_3_width * bin_3_count + bin_1_count + bin_2_count);

  EXPECT_EQ(histogram.estimate_cardinality(PredicateCondition::LessThan, "qrsu").type, EstimateType::MatchesExactly);
  EXPECT_FLOAT_EQ(histogram.estimate_cardinality(PredicateCondition::LessThan, "qrsu").cardinality,
                  bin_1_count + bin_2_count + bin_3_count);

  EXPECT_EQ(histogram.estimate_cardinality(PredicateCondition::LessThan, "uvwx").type, EstimateType::MatchesExactly);
  EXPECT_FLOAT_EQ(histogram.estimate_cardinality(PredicateCondition::LessThan, "uvwx").cardinality,
                  bin_1_count + bin_2_count + bin_3_count);

  EXPECT_EQ(histogram.estimate_cardinality(PredicateCondition::LessThan, "uvwy").type,
            EstimateType::MatchesApproximately);
  EXPECT_FLOAT_EQ(histogram.estimate_cardinality(PredicateCondition::LessThan, "uvwy").cardinality,
                  1 / bin_4_width * bin_4_count + bin_1_count + bin_2_count + bin_3_count);

  EXPECT_EQ(histogram.estimate_cardinality(PredicateCondition::LessThan, "uvwz").type,
            EstimateType::MatchesApproximately);
  EXPECT_FLOAT_EQ(histogram.estimate_cardinality(PredicateCondition::LessThan, "uvwz").cardinality,
                  2 / bin_4_width * bin_4_count + bin_1_count + bin_2_count + bin_3_count);

  EXPECT_EQ(histogram.estimate_cardinality(PredicateCondition::LessThan, "vvvv").type,
            EstimateType::MatchesApproximately);
  EXPECT_FLOAT_EQ(histogram.estimate_cardinality(PredicateCondition::LessThan, "vvvv").cardinality,
                  (21 * (ipow(26, 3) + ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1 +
                   21 * (ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1 + 21 * (ipow(26, 1) + ipow(26, 0)) + 1 +
                   21 * (ipow(26, 0)) + 1 - bin_4_lower) /
                          bin_4_width * bin_4_count +
                      bin_1_count + bin_2_count + bin_3_count);

  EXPECT_EQ(histogram.estimate_cardinality(PredicateCondition::LessThan, "xxxx").type,
            EstimateType::MatchesApproximately);
  EXPECT_FLOAT_EQ(histogram.estimate_cardinality(PredicateCondition::LessThan, "xxxx").cardinality,
                  (23 * (ipow(26, 3) + ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1 +
                   23 * (ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1 + 23 * (ipow(26, 1) + ipow(26, 0)) + 1 +
                   23 * (ipow(26, 0)) + 1 - bin_4_lower) /
                          bin_4_width * bin_4_count +
                      bin_1_count + bin_2_count + bin_3_count);

  EXPECT_EQ(histogram.estimate_cardinality(PredicateCondition::LessThan, "ycip").type,
            EstimateType::MatchesApproximately);
  EXPECT_FLOAT_EQ(histogram.estimate_cardinality(PredicateCondition::LessThan, "ycip").cardinality,
                  (24 * (ipow(26, 3) + ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1 +
                   2 * (ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1 + 8 * (ipow(26, 1) + ipow(26, 0)) + 1 +
                   15 * (ipow(26, 0)) + 1 - bin_4_lower) /
                          bin_4_width * bin_4_count +
                      bin_1_count + bin_2_count + bin_3_count);

  EXPECT_EQ(histogram.estimate_cardinality(PredicateCondition::LessThan, "yyzy").type,
            EstimateType::MatchesApproximately);
  EXPECT_FLOAT_EQ(histogram.estimate_cardinality(PredicateCondition::LessThan, "yyzy").cardinality,
                  (bin_4_width - 2) / bin_4_width * bin_4_count + bin_1_count + bin_2_count + bin_3_count);

  EXPECT_EQ(histogram.estimate_cardinality(PredicateCondition::LessThan, "yyzz").type,
            EstimateType::MatchesApproximately);
  EXPECT_FLOAT_EQ(histogram.estimate_cardinality(PredicateCondition::LessThan, "yyzz").cardinality,
                  (bin_4_width - 1) / bin_4_width * bin_4_count + bin_1_count + bin_2_count + bin_3_count);

  EXPECT_EQ(histogram.estimate_cardinality(PredicateCondition::LessThan, "yz").type, EstimateType::MatchesAll);
  EXPECT_FLOAT_EQ(histogram.estimate_cardinality(PredicateCondition::LessThan, "yz").cardinality, total_count);

  EXPECT_EQ(histogram.estimate_cardinality(PredicateCondition::LessThan, "zzzz").type, EstimateType::MatchesAll);
  EXPECT_FLOAT_EQ(histogram.estimate_cardinality(PredicateCondition::LessThan, "zzzz").cardinality, total_count);
}

TEST_F(GenericHistogramTest, StringLikeEstimation) {
  const auto histogram = GenericHistogram<pmr_string>{{"abcd", "ijkl", "oopp", "uvwx"},
                                                       {"efgh", "mnop", "qrst", "yyzz"},
                                                       {4, 6, 3, 3},
                                                       {3, 3, 3, 3},
                                                       StringHistogramDomain{'a', 'z', 4u}};

  // First bin: [abcd, efgh], so everything before is prunable.
  EXPECT_EQ(histogram.estimate_cardinality(PredicateCondition::Like, "a").type, EstimateType::MatchesNone);
  EXPECT_FLOAT_EQ(histogram.estimate_cardinality(PredicateCondition::Like, "a").cardinality, 0.f);

  EXPECT_EQ(histogram.estimate_cardinality(PredicateCondition::Like, "aa%").type, EstimateType::MatchesNone);
  EXPECT_FLOAT_EQ(histogram.estimate_cardinality(PredicateCondition::Like, "aa%").cardinality, 0.f);

  // Complexity of prefix pattern does not matter for pruning decision.
  EXPECT_EQ(histogram.estimate_cardinality(PredicateCondition::Like, "aa%zz%").type, EstimateType::MatchesNone);
  EXPECT_FLOAT_EQ(histogram.estimate_cardinality(PredicateCondition::Like, "aa%zz%").cardinality, 0.f);

  // Even though "aa%" is prunable, "a%" is not!
  EXPECT_EQ(histogram.estimate_cardinality(PredicateCondition::Like, "a%").type, EstimateType::MatchesApproximately);
  // Since there are no values smaller than "abcd", [abcd, azzz] is the range that "a%" covers.
  EXPECT_FLOAT_EQ(histogram.estimate_cardinality(PredicateCondition::Like, "a%").cardinality,
                  histogram.estimate_cardinality(PredicateCondition::LessThan, "b").cardinality -
                      histogram.estimate_cardinality(PredicateCondition::LessThan, "a").cardinality);
  EXPECT_FLOAT_EQ(histogram.estimate_cardinality(PredicateCondition::Like, "a%").cardinality,
                  histogram.estimate_cardinality(PredicateCondition::LessThan, "b").cardinality -
                      histogram.estimate_cardinality(PredicateCondition::LessThan, "abcd").cardinality);

  // No wildcard, no party.
  EXPECT_EQ(histogram.estimate_cardinality(PredicateCondition::Like, "abcd").type, EstimateType::MatchesApproximately);
  EXPECT_FLOAT_EQ(histogram.estimate_cardinality(PredicateCondition::Like, "abcd").cardinality,
                  histogram.estimate_cardinality(PredicateCondition::Equals, "abcd").cardinality);

  // Classic cases for prefix search.
  EXPECT_EQ(histogram.estimate_cardinality(PredicateCondition::Like, "ab%").type, EstimateType::MatchesApproximately);
  EXPECT_FLOAT_EQ(histogram.estimate_cardinality(PredicateCondition::Like, "ab%").cardinality,
                  histogram.estimate_cardinality(PredicateCondition::LessThan, "ac").cardinality -
                      histogram.estimate_cardinality(PredicateCondition::LessThan, "ab").cardinality);

  EXPECT_EQ(histogram.estimate_cardinality(PredicateCondition::Like, "c%").type, EstimateType::MatchesApproximately);
  EXPECT_FLOAT_EQ(histogram.estimate_cardinality(PredicateCondition::Like, "c%").cardinality,
                  histogram.estimate_cardinality(PredicateCondition::LessThan, "d").cardinality -
                      histogram.estimate_cardinality(PredicateCondition::LessThan, "c").cardinality);

  EXPECT_EQ(histogram.estimate_cardinality(PredicateCondition::Like, "cfoo%").type, EstimateType::MatchesApproximately);
  EXPECT_FLOAT_EQ(histogram.estimate_cardinality(PredicateCondition::Like, "cfoo%").cardinality,
                  histogram.estimate_cardinality(PredicateCondition::LessThan, "cfop").cardinality -
                      histogram.estimate_cardinality(PredicateCondition::LessThan, "cfoo").cardinality);

  // Use upper bin boundary as range limit, since there are no other values starting with e in other bins.
  EXPECT_EQ(histogram.estimate_cardinality(PredicateCondition::Like, "e%").type, EstimateType::MatchesApproximately);
  EXPECT_FLOAT_EQ(histogram.estimate_cardinality(PredicateCondition::Like, "e%").cardinality,
                  histogram.estimate_cardinality(PredicateCondition::LessThan, "f").cardinality -
                      histogram.estimate_cardinality(PredicateCondition::LessThan, "e").cardinality);
  EXPECT_FLOAT_EQ(histogram.estimate_cardinality(PredicateCondition::Like, "e%").cardinality,
                  histogram.estimate_cardinality(PredicateCondition::LessThanEquals, "efgh").cardinality -
                      histogram.estimate_cardinality(PredicateCondition::LessThan, "e").cardinality);

  // Second bin starts at ijkl, so there is a gap between efgh and ijkl.
  EXPECT_EQ(histogram.estimate_cardinality(PredicateCondition::Like, "f%").type, EstimateType::MatchesNone);
  EXPECT_FLOAT_EQ(histogram.estimate_cardinality(PredicateCondition::Like, "f%").cardinality, 0.f);

  EXPECT_EQ(histogram.estimate_cardinality(PredicateCondition::Like, "ii%").type, EstimateType::MatchesNone);
  EXPECT_FLOAT_EQ(histogram.estimate_cardinality(PredicateCondition::Like, "ii%").cardinality, 0.f);

  EXPECT_EQ(histogram.estimate_cardinality(PredicateCondition::Like, "iizzzzzzzz%").type, EstimateType::MatchesNone);
  EXPECT_FLOAT_EQ(histogram.estimate_cardinality(PredicateCondition::Like, "iizzzzzzzz%").cardinality, 0.f);

  EXPECT_EQ(histogram.estimate_cardinality(PredicateCondition::Like, "%").type, EstimateType::MatchesAll);
  EXPECT_EQ(histogram.estimate_cardinality(PredicateCondition::Like, "z%foo").type, EstimateType::MatchesNone);
  EXPECT_EQ(histogram.estimate_cardinality(PredicateCondition::Like, "z%foo%").type, EstimateType::MatchesNone);

  EXPECT_EQ(histogram.estimate_cardinality(PredicateCondition::Like, "%a%").cardinality,
            histogram.total_count() / ipow(26, 1));
  EXPECT_EQ(histogram.estimate_cardinality(PredicateCondition::Like, "%a%b").cardinality,
            histogram.total_count() / ipow(26, 2));
  EXPECT_EQ(histogram.estimate_cardinality(PredicateCondition::Like, "foo%bar").cardinality,
            histogram.estimate_cardinality(PredicateCondition::Like, "foo%").cardinality / ipow(26, 3));
  EXPECT_EQ(histogram.estimate_cardinality(PredicateCondition::Like, "foo%bar%").cardinality,
            histogram.estimate_cardinality(PredicateCondition::Like, "foo%").cardinality / ipow(26, 3));

  // If the number of fixed characters is too large and the power would overflow, cap it.
  EXPECT_EQ(histogram.estimate_cardinality(PredicateCondition::Like, "foo%bar%baz%qux%quux").cardinality,
            histogram.estimate_cardinality(PredicateCondition::Like, "foo%").cardinality / ipow(26, 13));
  EXPECT_EQ(histogram.estimate_cardinality(PredicateCondition::Like, "foo%bar%baz%qux%quux%corge").cardinality,
            histogram.estimate_cardinality(PredicateCondition::Like, "foo%").cardinality / ipow(26, 13));
}

TEST_F(GenericHistogramTest, StringNotLikeEstimation) {
  const auto histogram = GenericHistogram<pmr_string>{{"abcd", "ijkl", "oopp", "uvwx"},
                                                       {"efgh", "mnop", "qrst", "yyzz"},
                                                       {4, 6, 3, 3},
                                                       {3, 3, 3, 3},
                                                       StringHistogramDomain{'a', 'z', 4u}};

  EXPECT_EQ(histogram.estimate_cardinality(PredicateCondition::NotLike, "%").cardinality, 0.f);
  EXPECT_EQ(histogram.estimate_cardinality(PredicateCondition::NotLike, "%a").type, EstimateType::MatchesApproximately);
  EXPECT_EQ(histogram.estimate_cardinality(PredicateCondition::NotLike, "%c").type, EstimateType::MatchesApproximately);
  EXPECT_EQ(histogram.estimate_cardinality(PredicateCondition::NotLike, "a%").type, EstimateType::MatchesApproximately);

  EXPECT_EQ(histogram.estimate_cardinality(PredicateCondition::NotLike, "aa%").type, EstimateType::MatchesAll);
  EXPECT_EQ(histogram.estimate_cardinality(PredicateCondition::NotLike, "z%").type, EstimateType::MatchesAll);
  EXPECT_EQ(histogram.estimate_cardinality(PredicateCondition::NotLike, "z%foo").type, EstimateType::MatchesAll);
  EXPECT_EQ(histogram.estimate_cardinality(PredicateCondition::NotLike, "z%foo%").type, EstimateType::MatchesAll);
}

TEST_F(GenericHistogramTest, NotLikePruningSpecial) {
  const auto histogram = GenericHistogram<pmr_string>{
      {"dampf", "dampfschifffahrtsgeselle", "dampfschifffahrtsgesellschaftskapitaen"},
      {"dampfschifffahrt", "dampfschifffahrtsgesellschaft", "dampfschifffahrtsgesellschaftskapitaensdampf"},
      {3, 2, 2},
      {3, 2, 2},
      StringHistogramDomain{'a', 'z', 4u}};

  EXPECT_EQ(histogram.estimate_cardinality(PredicateCondition::NotLike, "d%").type, EstimateType::MatchesNone);
  EXPECT_EQ(histogram.estimate_cardinality(PredicateCondition::NotLike, "da%").type, EstimateType::MatchesNone);
  EXPECT_EQ(histogram.estimate_cardinality(PredicateCondition::NotLike, "dam%").type, EstimateType::MatchesNone);
  EXPECT_EQ(histogram.estimate_cardinality(PredicateCondition::NotLike, "damp%").type, EstimateType::MatchesNone);
  EXPECT_EQ(histogram.estimate_cardinality(PredicateCondition::NotLike, "dampf%").type, EstimateType::MatchesNone);

  EXPECT_EQ(histogram.estimate_cardinality(PredicateCondition::NotLike, "dampfs%").type,
            EstimateType::MatchesApproximately);
  EXPECT_EQ(histogram.estimate_cardinality(PredicateCondition::NotLike, "dampfschifffahrtsgesellschaft%").type,
            EstimateType::MatchesApproximately);
  EXPECT_EQ(histogram.estimate_cardinality(PredicateCondition::NotLike, "db%").type, EstimateType::MatchesAll);
  EXPECT_EQ(histogram.estimate_cardinality(PredicateCondition::NotLike, "e%").type, EstimateType::MatchesAll);
}

TEST_F(GenericHistogramTest, IntBetweenPruning) {
  const auto histogram = GenericHistogram<int32_t>{{12, 12345}, {123, 123456}, {2, 5}, {2, 2}};

  EXPECT_EQ(histogram.estimate_cardinality(PredicateCondition::Between, 124, 12344).type, EstimateType::MatchesNone);
  EXPECT_EQ(histogram.estimate_cardinality(PredicateCondition::Between, 0, 11).type, EstimateType::MatchesNone);
  EXPECT_EQ(histogram.estimate_cardinality(PredicateCondition::Between, 50, 60).type,
            EstimateType::MatchesApproximately);
  EXPECT_EQ(histogram.estimate_cardinality(PredicateCondition::Between, 123, 124).type,
            EstimateType::MatchesApproximately);
  EXPECT_EQ(histogram.estimate_cardinality(PredicateCondition::Between, 124, 12'344).type, EstimateType::MatchesNone);
  EXPECT_EQ(histogram.estimate_cardinality(PredicateCondition::Between, 12'344, 12'344).type,
            EstimateType::MatchesNone);
  EXPECT_EQ(histogram.estimate_cardinality(PredicateCondition::Between, 12'344, 12'345).type,
            EstimateType::MatchesApproximately);
}

TEST_F(GenericHistogramTest, IntBetweenPruningSpecial) {
  const auto histogram = GenericHistogram<int32_t>{{12}, {123456}, {7}, {4}};

  // Make sure that pruning does not do anything stupid with one bin.
  EXPECT_EQ(histogram.estimate_cardinality(PredicateCondition::Between, 0, 1'000'000).type, EstimateType::MatchesAll);
}

TEST_F(GenericHistogramTest, StringLikeEdgePruning) {
  /**
   * This test makes sure that pruning works even if the next value after the search prefix is part of a bin.
   * In this case, we check "d%", which is handled as the range [d, e).
   * "e" is the lower edge of the bin that appears after the gap in which "d" falls.
   * A similar situation arises for "v%", but "w" should also be in a gap,
   * because the last bin starts with the next value after "w", i.e., "wa".
   * bins: [aa, bums], [e, uuu], [wa, zzz]
   * For more details see AbstractHistogram::does_not_contain.
   * We test all the other one-letter prefixes as well, because, why not.
   */
  const auto histogram = GenericHistogram<pmr_string>{{"aa", "e", "wa"}, {"bums", "uuu", "zzz"}, {4, 6, 6}, {4, 4, 4}};

  // Not prunable, because values start with the character.
  EXPECT_EQ(histogram.estimate_cardinality(PredicateCondition::Like, "a%").type, EstimateType::MatchesApproximately);
  EXPECT_EQ(histogram.estimate_cardinality(PredicateCondition::Like, "b%").type, EstimateType::MatchesApproximately);

  // Prunable, because in a gap.
  EXPECT_EQ(histogram.estimate_cardinality(PredicateCondition::Like, "c%").type, EstimateType::MatchesNone);

  // This is the interesting part.
  EXPECT_EQ(histogram.estimate_cardinality(PredicateCondition::Like, "d%").type, EstimateType::MatchesNone);

  // Not prunable, because bin range is [e, uuu].
  EXPECT_EQ(histogram.estimate_cardinality(PredicateCondition::Like, "e%").type, EstimateType::MatchesApproximately);
  EXPECT_EQ(histogram.estimate_cardinality(PredicateCondition::Like, "f%").type, EstimateType::MatchesApproximately);
  EXPECT_EQ(histogram.estimate_cardinality(PredicateCondition::Like, "g%").type, EstimateType::MatchesApproximately);
  EXPECT_EQ(histogram.estimate_cardinality(PredicateCondition::Like, "h%").type, EstimateType::MatchesApproximately);
  EXPECT_EQ(histogram.estimate_cardinality(PredicateCondition::Like, "i%").type, EstimateType::MatchesApproximately);
  EXPECT_EQ(histogram.estimate_cardinality(PredicateCondition::Like, "j%").type, EstimateType::MatchesApproximately);
  EXPECT_EQ(histogram.estimate_cardinality(PredicateCondition::Like, "k%").type, EstimateType::MatchesApproximately);
  EXPECT_EQ(histogram.estimate_cardinality(PredicateCondition::Like, "l%").type, EstimateType::MatchesApproximately);
  EXPECT_EQ(histogram.estimate_cardinality(PredicateCondition::Like, "m%").type, EstimateType::MatchesApproximately);
  EXPECT_EQ(histogram.estimate_cardinality(PredicateCondition::Like, "n%").type, EstimateType::MatchesApproximately);
  EXPECT_EQ(histogram.estimate_cardinality(PredicateCondition::Like, "o%").type, EstimateType::MatchesApproximately);
  EXPECT_EQ(histogram.estimate_cardinality(PredicateCondition::Like, "p%").type, EstimateType::MatchesApproximately);
  EXPECT_EQ(histogram.estimate_cardinality(PredicateCondition::Like, "q%").type, EstimateType::MatchesApproximately);
  EXPECT_EQ(histogram.estimate_cardinality(PredicateCondition::Like, "r%").type, EstimateType::MatchesApproximately);
  EXPECT_EQ(histogram.estimate_cardinality(PredicateCondition::Like, "s%").type, EstimateType::MatchesApproximately);
  EXPECT_EQ(histogram.estimate_cardinality(PredicateCondition::Like, "t%").type, EstimateType::MatchesApproximately);
  EXPECT_EQ(histogram.estimate_cardinality(PredicateCondition::Like, "u%").type, EstimateType::MatchesApproximately);

  // The second more interesting test.
  EXPECT_EQ(histogram.estimate_cardinality(PredicateCondition::Like, "v%").type, EstimateType::MatchesNone);

  // Not prunable, because bin range is [wa, zzz].
  EXPECT_EQ(histogram.estimate_cardinality(PredicateCondition::Like, "w%").type, EstimateType::MatchesApproximately);
  EXPECT_EQ(histogram.estimate_cardinality(PredicateCondition::Like, "x%").type, EstimateType::MatchesApproximately);
  EXPECT_EQ(histogram.estimate_cardinality(PredicateCondition::Like, "y%").type, EstimateType::MatchesApproximately);
  EXPECT_EQ(histogram.estimate_cardinality(PredicateCondition::Like, "z%").type, EstimateType::MatchesApproximately);
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
  EXPECT_EQ(histogram.estimate_cardinality(PredicateCondition::Equals, 1).cardinality, 0.0f);
  EXPECT_EQ(histogram.estimate_cardinality(PredicateCondition::Equals, 3).cardinality, 17.0f / 5.0f);
  EXPECT_EQ(histogram.estimate_cardinality(PredicateCondition::Equals, 26).cardinality, 0.0f);
  EXPECT_EQ(histogram.estimate_cardinality(PredicateCondition::Equals, 105).cardinality, 5.0f);
  EXPECT_EQ(histogram.estimate_cardinality(PredicateCondition::Equals, 200).cardinality, 0.0f);
  EXPECT_EQ(histogram_zeros.estimate_cardinality(PredicateCondition::Equals, 2).cardinality, 0.0f);
  EXPECT_EQ(histogram_zeros.estimate_cardinality(PredicateCondition::Equals, 21).cardinality, 0.0f);
  EXPECT_EQ(histogram_zeros.estimate_cardinality(PredicateCondition::Equals, 37).cardinality, 0.0f);

  EXPECT_EQ(histogram.estimate_cardinality(PredicateCondition::NotEquals, 1).cardinality, total_count);
  EXPECT_EQ(histogram.estimate_cardinality(PredicateCondition::NotEquals, 21).cardinality, total_count - 10);
  EXPECT_EQ(histogram_zeros.estimate_cardinality(PredicateCondition::NotEquals, 2).cardinality, 6.0f);
  EXPECT_EQ(histogram_zeros.estimate_cardinality(PredicateCondition::NotEquals, 21).cardinality, 6.0f);
  EXPECT_EQ(histogram_zeros.estimate_cardinality(PredicateCondition::NotEquals, 37).cardinality, 6.0f);

  EXPECT_EQ(histogram.estimate_cardinality(PredicateCondition::LessThan, -10).cardinality, 0.0f);
  EXPECT_EQ(histogram.estimate_cardinality(PredicateCondition::LessThan, 2).cardinality, 0.0f);
  EXPECT_EQ(histogram.estimate_cardinality(PredicateCondition::LessThan, 20).cardinality, 17.0f - 17.0f / 19.0f);  // NOLINT
  EXPECT_EQ(histogram.estimate_cardinality(PredicateCondition::LessThan, 21).cardinality, 17.0f);
  EXPECT_EQ(histogram.estimate_cardinality(PredicateCondition::LessThan, 40).cardinality, 17.0f + 30 + 3 * (40.0f / 64.0f));  // NOLINT
  EXPECT_EQ(histogram.estimate_cardinality(PredicateCondition::LessThan, 105).cardinality, total_count - 5);
  EXPECT_EQ(histogram.estimate_cardinality(PredicateCondition::LessThan, 1000).cardinality, total_count);

  EXPECT_EQ(histogram.estimate_cardinality(PredicateCondition::LessThanEquals, -10).cardinality, 0.0f);
  EXPECT_EQ(histogram.estimate_cardinality(PredicateCondition::LessThanEquals, 2).cardinality, 17.0f / 19.0f);
  EXPECT_EQ(histogram.estimate_cardinality(PredicateCondition::LessThanEquals, 3).cardinality, 2 * (17.0f / 19.0f));  // NOLINT
  EXPECT_EQ(histogram.estimate_cardinality(PredicateCondition::LessThanEquals, 20).cardinality, 17.0f);
  EXPECT_EQ(histogram.estimate_cardinality(PredicateCondition::LessThanEquals, 21).cardinality, 17.0f + (30.0f / 5.0f));  // NOLINT
  EXPECT_EQ(histogram.estimate_cardinality(PredicateCondition::LessThanEquals, 40).cardinality, 17.0f + 30 + 4 * (40.0f / 64.0f));  // NOLINT
  EXPECT_EQ(histogram.estimate_cardinality(PredicateCondition::LessThanEquals, 105).cardinality, total_count);  // NOLINT
  EXPECT_EQ(histogram.estimate_cardinality(PredicateCondition::LessThanEquals, 1000).cardinality, total_count);  // NOLINT

  EXPECT_EQ(histogram.estimate_cardinality(PredicateCondition::GreaterThan, -10).cardinality, total_count);
  EXPECT_EQ(histogram.estimate_cardinality(PredicateCondition::GreaterThan, 1).cardinality, total_count);
  EXPECT_EQ(histogram.estimate_cardinality(PredicateCondition::GreaterThan, 2).cardinality, total_count - (17.0f / 19.0f));  // NOLINT
  EXPECT_EQ(histogram.estimate_cardinality(PredicateCondition::GreaterThan, 20).cardinality, 76.0f);
  EXPECT_EQ(histogram.estimate_cardinality(PredicateCondition::GreaterThan, 21).cardinality, 76.0f - (30.0f / 5.0f));  // NOLINT
  EXPECT_EQ(histogram.estimate_cardinality(PredicateCondition::GreaterThan, 105).cardinality, 0.0f);
  EXPECT_EQ(histogram.estimate_cardinality(PredicateCondition::GreaterThan, 1000).cardinality, 0.0f);

  EXPECT_EQ(histogram.estimate_cardinality(PredicateCondition::GreaterThanEquals, -10).cardinality, total_count);  // NOLINT
  EXPECT_EQ(histogram.estimate_cardinality(PredicateCondition::GreaterThanEquals, 1).cardinality, total_count);  // NOLINT
  EXPECT_EQ(histogram.estimate_cardinality(PredicateCondition::GreaterThanEquals, 2).cardinality, total_count);  // NOLINT
  EXPECT_EQ(histogram.estimate_cardinality(PredicateCondition::GreaterThanEquals, 20).cardinality, 76.0f + 17.0f / 19.0f);  // NOLINT
  EXPECT_EQ(histogram.estimate_cardinality(PredicateCondition::GreaterThanEquals, 21).cardinality, 76.0f);
  EXPECT_EQ(histogram.estimate_cardinality(PredicateCondition::GreaterThanEquals, 105).cardinality, 5.0f);
  EXPECT_EQ(histogram.estimate_cardinality(PredicateCondition::GreaterThanEquals, 1000).cardinality, 0.0f);

  EXPECT_EQ(histogram.estimate_cardinality(PredicateCondition::Between, 2, 20).cardinality, 17.0f);
  EXPECT_EQ(histogram.estimate_cardinality(PredicateCondition::Between, 2, 25).cardinality, 47.0f);
  EXPECT_EQ(histogram.estimate_cardinality(PredicateCondition::Between, 26, 27).cardinality, 0.0f);
  EXPECT_EQ(histogram.estimate_cardinality(PredicateCondition::Between, 105, 105).cardinality, 5);
  EXPECT_EQ(histogram.estimate_cardinality(PredicateCondition::Between, 105, 106).cardinality, 5);
  EXPECT_EQ(histogram.estimate_cardinality(PredicateCondition::Between, 107, 107).cardinality, 0.0f);
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
  EXPECT_EQ(histogram->estimate_cardinality(PredicateCondition::Equals, 1.0f).cardinality, 0.0f);
  EXPECT_EQ(histogram->estimate_cardinality(PredicateCondition::Equals, 3.0f).cardinality, 17.f / 5.0f);
  EXPECT_EQ(histogram->estimate_cardinality(PredicateCondition::Equals, 22.5f).cardinality, 0.0f);
  EXPECT_EQ(histogram->estimate_cardinality(PredicateCondition::Equals, 31.0f).cardinality, 7.0f / 2.0f);
  EXPECT_EQ(histogram->estimate_cardinality(PredicateCondition::Equals, next_value(31.0f)).cardinality, 7.0f / 2.0f);
  EXPECT_EQ(histogram->estimate_cardinality(PredicateCondition::Equals, 32.0f).cardinality, 3.0f);

  EXPECT_EQ(histogram->estimate_cardinality(PredicateCondition::NotEquals, 1.0f).cardinality, total_count);
  EXPECT_EQ(histogram->estimate_cardinality(PredicateCondition::NotEquals, 2.0f).cardinality, total_count - 17.0f / 5.0f);  // NOLINT
  EXPECT_EQ(histogram->estimate_cardinality(PredicateCondition::NotEquals, 31.0f).cardinality, total_count - 7.0f / 2.0f);
  EXPECT_EQ(histogram->estimate_cardinality(PredicateCondition::NotEquals, next_value(31.0f)).cardinality, total_count - 7.0f / 2.0f);
  EXPECT_EQ(histogram->estimate_cardinality(PredicateCondition::NotEquals, 32.0f).cardinality, 74);

  EXPECT_EQ(histogram->estimate_cardinality(PredicateCondition::LessThan, 2.0f).cardinality, 0.0f);
  EXPECT_EQ(histogram->estimate_cardinality(PredicateCondition::LessThan, next_value(2.0f)).cardinality, 17.0f * ((next_value(2.0f) - 2.0f) / 20.0f));  // NOLINT
  // Floating point quirk: These go to exactly 67, should be slightly below
  EXPECT_EQ(histogram->estimate_cardinality(PredicateCondition::LessThan, 24.5f).cardinality, 17.0f + 30 * (1.5f / 2.0f)); // NOLINT
  EXPECT_EQ(histogram->estimate_cardinality(PredicateCondition::LessThan, 30.0f).cardinality, 67.0f);
  EXPECT_EQ(histogram->estimate_cardinality(PredicateCondition::LessThan, next_value(30.0f)).cardinality, 67.0f);
  EXPECT_EQ(histogram->estimate_cardinality(PredicateCondition::LessThan, 150.0f).cardinality, total_count);
  EXPECT_EQ(histogram->estimate_cardinality(PredicateCondition::LessThan, 31.0f).cardinality, 67.0f);
  // Floating point quirk: The bin [31, next_value(31.0f)] is too small to be split at `< next_value(31.0f)`
  EXPECT_EQ(histogram->estimate_cardinality(PredicateCondition::LessThan, next_value(31.0f)).cardinality, 74.0f);
  EXPECT_EQ(histogram->estimate_cardinality(PredicateCondition::LessThan, 32.0f).cardinality, 74.0f);
  EXPECT_EQ(histogram->estimate_cardinality(PredicateCondition::LessThan, next_value(32.0f)).cardinality, 77.0f);

  EXPECT_EQ(histogram->estimate_cardinality(PredicateCondition::LessThanEquals, 2.0f).cardinality, 17.0f * ((next_value(2.0f) - 2.0f) / 20.0f));  // NOLINT
  EXPECT_EQ(histogram->estimate_cardinality(PredicateCondition::LessThanEquals, previous_value(24.0f)).cardinality, 17.0f + 30.0f * 0.5f);
  EXPECT_EQ(histogram->estimate_cardinality(PredicateCondition::LessThanEquals, 30.0f).cardinality, 67.0f);
  EXPECT_EQ(histogram->estimate_cardinality(PredicateCondition::LessThanEquals, 150.0f).cardinality, total_count);  // NOLINT
  // Floating point quirk: `<= 31.0f` is `< next_value(31.0f)`, which in turn covers the entire bin.
  EXPECT_EQ(histogram->estimate_cardinality(PredicateCondition::LessThanEquals, 31.0f).cardinality, 74.0f);
  EXPECT_EQ(histogram->estimate_cardinality(PredicateCondition::LessThanEquals, next_value(31.0f)).cardinality, 74.0f);  // NOLINT
  EXPECT_EQ(histogram->estimate_cardinality(PredicateCondition::LessThanEquals, 32.0f).cardinality, total_count);
  EXPECT_EQ(histogram->estimate_cardinality(PredicateCondition::LessThanEquals, next_value(32.0f)).cardinality, total_count);  // NOLINT

  EXPECT_EQ(histogram->estimate_cardinality(PredicateCondition::GreaterThan, 2.0f).cardinality, total_count - 17.0f * ((next_value(2.0f) - 2.0f) / 20.0f));  // NOLINT
  EXPECT_EQ(histogram->estimate_cardinality(PredicateCondition::GreaterThan, 30.0f).cardinality, 10.0f);
  EXPECT_EQ(histogram->estimate_cardinality(PredicateCondition::GreaterThan, 150.0f).cardinality, 0.0f);
  EXPECT_EQ(histogram->estimate_cardinality(PredicateCondition::GreaterThan, 31.0f).cardinality, 3.0f);
  EXPECT_EQ(histogram->estimate_cardinality(PredicateCondition::GreaterThan, next_value(31.0f)).cardinality, 3.0f);  // NOLINT
  EXPECT_EQ(histogram->estimate_cardinality(PredicateCondition::GreaterThan, 32.0f).cardinality, 0.0f);
  EXPECT_EQ(histogram->estimate_cardinality(PredicateCondition::GreaterThan, previous_value(32.0f)).cardinality, 3);  // NOLINT

  EXPECT_EQ(histogram->estimate_cardinality(PredicateCondition::GreaterThanEquals, 2.0f).cardinality, total_count);  // NOLINT
  EXPECT_EQ(histogram->estimate_cardinality(PredicateCondition::GreaterThanEquals, 24.0f).cardinality, 45.0f);
  EXPECT_EQ(histogram->estimate_cardinality(PredicateCondition::GreaterThanEquals, 30.0f).cardinality, 10.0f);
  EXPECT_EQ(histogram->estimate_cardinality(PredicateCondition::GreaterThanEquals, 150.0f).cardinality, 0.0f);
  EXPECT_EQ(histogram->estimate_cardinality(PredicateCondition::GreaterThanEquals, 31.0f).cardinality, 10.0f);
  EXPECT_EQ(histogram->estimate_cardinality(PredicateCondition::GreaterThanEquals, next_value(31.0f)).cardinality, 3.0f);  // NOLINT
  EXPECT_EQ(histogram->estimate_cardinality(PredicateCondition::GreaterThanEquals, 32.0f).cardinality, 3.0f);
  EXPECT_EQ(histogram->estimate_cardinality(PredicateCondition::GreaterThanEquals, previous_value(32.0f)).cardinality, 3);  // NOLINT

  EXPECT_FLOAT_EQ(histogram->estimate_cardinality(PredicateCondition::Between, 2.0f, 3.0f).cardinality,  17.0f * ((next_value(3.0f) - 2.0f) / 20.0f));  // NOLINT
  EXPECT_FLOAT_EQ(histogram->estimate_cardinality(PredicateCondition::Between, 2.0f, next_value(2.0f)).cardinality, 0.0f);  // NOLINT
  EXPECT_EQ(histogram->estimate_cardinality(PredicateCondition::Between, 2.0f, 22.5f).cardinality, 17.0f);  // NOLINT
  EXPECT_EQ(histogram->estimate_cardinality(PredicateCondition::Between, 2.0f, 30.0f).cardinality, 67.0f);  // NOLINT
  EXPECT_FLOAT_EQ(histogram->estimate_cardinality(PredicateCondition::Between, previous_value(2.0f), 2.0f).cardinality, 0.0f);  // NOLINT
  // clang-format on
}

TEST_F(GenericHistogramTest, EstimateCardinalityString) {
  CardinalityEstimate estimate;

  // clang-format off
  const auto histogram = std::make_shared<GenericHistogram<pmr_string>>(
  std::vector<pmr_string>        {"aa", "at", "bi"},
  std::vector<pmr_string>        {"as", "ax", "dr"},
  std::vector<HistogramCountType>{  17,   30,   40},
  std::vector<HistogramCountType>{   5,    3,   27},
  StringHistogramDomain{'a', 'z', 2u});
  // clang-format on

  estimate = histogram->estimate_cardinality(PredicateCondition::Equals, "a");
  EXPECT_FLOAT_EQ(estimate.cardinality, 0.f);
  EXPECT_EQ(estimate.type, EstimateType::MatchesNone);

  estimate = histogram->estimate_cardinality(PredicateCondition::Equals, "ab");
  EXPECT_FLOAT_EQ(estimate.cardinality, 17.f / 5);
  EXPECT_EQ(estimate.type, EstimateType::MatchesApproximately);

  estimate = histogram->estimate_cardinality(PredicateCondition::Equals, "ay");
  EXPECT_FLOAT_EQ(estimate.cardinality, 0.f);
  EXPECT_EQ(estimate.type, EstimateType::MatchesNone);
}

TEST_F(GenericHistogramTest, SlicedInt) {
  // clang-format off

  // Normal histogram, no special cases here
  const auto histogram_a = std::make_shared<GenericHistogram<int32_t>>(
    std::vector<int32_t>            { 1, 31, 60, 80},
    std::vector<int32_t>            {25, 50, 60, 99},
    std::vector<HistogramCountType> {40, 30, 5,  10},
    std::vector<HistogramCountType> {10, 20, 1,  1}
  );

  // Histogram with zeros in bin height/distinct_count
  const auto histogram_b = std::make_shared<GenericHistogram<int32_t>>(
    std::vector<int32_t>            { 0,  6, 30, 51},
    std::vector<int32_t>            { 5, 20, 50, 52},
    std::vector<HistogramCountType> { 4,  0,  0,  0.000001f},
    std::vector<HistogramCountType> { 0, 20,  0,  0.000001f}
  );

  // Histogram with a single bin, which in turn has a single value
  const auto histogram_c = std::make_shared<GenericHistogram<int32_t>>(
    std::vector<int32_t>            { 5},
    std::vector<int32_t>            {15},
    std::vector<HistogramCountType> { 1},
    std::vector<HistogramCountType> { 1}
  );

  // Empty histogram
  const auto histogram_d = std::make_shared<GenericHistogram<int32_t>>(
    std::vector<int32_t>            {},
    std::vector<int32_t>            {},
    std::vector<HistogramCountType> {},
    std::vector<HistogramCountType> {}
  );
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
      Predicate{PredicateCondition::Between, 0, 0},
      Predicate{PredicateCondition::Between, 26, 29},
      Predicate{PredicateCondition::Between, 100, 1000},
      Predicate{PredicateCondition::Between, 1, 20},
      Predicate{PredicateCondition::Between, 1, 50},
      Predicate{PredicateCondition::Between, 21, 60},
      Predicate{PredicateCondition::Between, 60, 60},
      Predicate{PredicateCondition::Between, 32, 99},
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
    std::vector<HistogramCountType>{1,               5,      4,    70,             1}
  );

  // Histogram with zeros in bin height/distinct_count
  const auto histogram_b = std::make_shared<GenericHistogram<float>>(
  std::vector<float>              { 0,  6, 30, 51},
  std::vector<float>              { 5, 20, 50, 52},
  std::vector<HistogramCountType> { 4,  0,  0,  0.000001f},
  std::vector<HistogramCountType> { 0, 20,  0,  0.000001f}
  );

  const auto histogram_c = std::make_shared<GenericHistogram<float>>(
    std::vector<float>             {5.0f},
    std::vector<float>             {5.0f},
    std::vector<HistogramCountType>{12},
    std::vector<HistogramCountType>{1}
  );

  const auto histogram_d = std::make_shared<GenericHistogram<float>>(
    std::vector<float>             {},
    std::vector<float>             {},
    std::vector<HistogramCountType>{},
    std::vector<HistogramCountType>{}
  );
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
      Predicate{PredicateCondition::Between, 0.0f, 1'000'100.0f},
      Predicate{PredicateCondition::Between, 1.0f, 1'000'100.0f},
      Predicate{PredicateCondition::Between, 2.0f, 1'000'100.0f},
      Predicate{PredicateCondition::Between, 2.0f, 50.0f},
      Predicate{PredicateCondition::Between, 3.0f, 11.00f},
      Predicate{PredicateCondition::Between, 10.0f, 50.0f},
      Predicate{PredicateCondition::Between, 20.0f, 50.0f},
  };

  test_sliced_with_predicates(histograms, predicates);
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
  const auto histogram_2_expected_minima = std::vector<int32_t>{   2, 5,     12, 15, 19, 20,         40, 41, 45, 50, 52};
  const auto histogram_1_expected_maxima = std::vector<int32_t>{1, 4, 7, 10,     18,     25, 29, 39, 40,     48, 51};
  const auto histogram_2_expected_maxima = std::vector<int32_t>{   4, 7,     14, 18, 19, 25,         40, 42, 48, 51, 52};
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
