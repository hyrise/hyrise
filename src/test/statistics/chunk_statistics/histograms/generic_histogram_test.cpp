#include <limits>
#include <memory>
#include <string>

#include "base_test.hpp"
#include "gtest/gtest.h"

#include "statistics/chunk_statistics/histograms/generic_histogram.hpp"
#include "statistics/chunk_statistics/histograms/histogram_utils.hpp"
#include "utils/load_table.hpp"

namespace opossum {

class GenericHistogramTest : public BaseTest {
  void SetUp() override {
    // clang-format off
    _int_histogram = std::make_shared<GenericHistogram<int32_t>>(
            std::vector<int32_t>{2,  21, 37},
            std::vector<int32_t>{20, 25, 100},
            std::vector<HistogramCountType>{17, 30, 40},
            std::vector<HistogramCountType>{5,  3,  27}
    );
    _double_histogram = std::make_shared<GenericHistogram<double>>(
            std::vector<double>{2.,  21., 37.},
            std::vector<double>{20., 25., 100.},
            std::vector<HistogramCountType>{17, 30, 40},
            std::vector<HistogramCountType>{5,  3,  27}
    );
    _string_histogram = std::make_shared<GenericHistogram<std::string>>(
            std::vector<std::string>{"b",  "at", "bi"},
            std::vector<std::string>{"as", "ax", "dr"},
            std::vector<HistogramCountType>{17, 30, 40},
            std::vector<HistogramCountType>{5,  3,  27}
    );
    // clang-format on
  }

 protected:
  std::shared_ptr<GenericHistogram<int32_t>> _int_histogram;
  std::shared_ptr<GenericHistogram<double>> _double_histogram;
  std::shared_ptr<GenericHistogram<std::string>> _string_histogram;
};

TEST_F(GenericHistogramTest, Basic) {
  EXPECT_TRUE(_int_histogram->can_prune(PredicateCondition::Equals, AllTypeVariant{1}));
  EXPECT_TRUE(_double_histogram->can_prune(PredicateCondition::Equals, AllTypeVariant{1.}));
  EXPECT_TRUE(_string_histogram->can_prune(PredicateCondition::Equals, AllTypeVariant{"a"}));
  EXPECT_FLOAT_EQ(_int_histogram->estimate_cardinality(PredicateCondition::Equals, 1), 0.f);
  EXPECT_FLOAT_EQ(_double_histogram->estimate_cardinality(PredicateCondition::Equals, 1.), 0.f);
  EXPECT_FLOAT_EQ(_string_histogram->estimate_cardinality(PredicateCondition::Equals, "a"), 0.f);

  EXPECT_FALSE(_int_histogram->can_prune(PredicateCondition::Equals, AllTypeVariant{3}));
  EXPECT_FALSE(_double_histogram->can_prune(PredicateCondition::Equals, AllTypeVariant{3.}));
  EXPECT_FALSE(_string_histogram->can_prune(PredicateCondition::Equals, AllTypeVariant{"ab"}));
  EXPECT_FLOAT_EQ(_int_histogram->estimate_cardinality(PredicateCondition::Equals, 3), 19.f / 5);
  EXPECT_FLOAT_EQ(_double_histogram->estimate_cardinality(PredicateCondition::Equals, 3.), 19.f / 5);
  EXPECT_FLOAT_EQ(_string_histogram->estimate_cardinality(PredicateCondition::Equals, "ab"), 19.f / 5);

  EXPECT_TRUE(_int_histogram->can_prune(PredicateCondition::Equals, AllTypeVariant{26}));
  EXPECT_TRUE(_double_histogram->can_prune(PredicateCondition::Equals, AllTypeVariant{26.}));
  EXPECT_TRUE(_string_histogram->can_prune(PredicateCondition::Equals, AllTypeVariant{"ay"}));
  EXPECT_FLOAT_EQ(_int_histogram->estimate_cardinality(PredicateCondition::Equals, 26), 0.f);
  EXPECT_FLOAT_EQ(_double_histogram->estimate_cardinality(PredicateCondition::Equals, 26.), 0.f);
  EXPECT_FLOAT_EQ(_string_histogram->estimate_cardinality(PredicateCondition::Equals, "ay"), 0.f);
}

}  // namespace opossum
