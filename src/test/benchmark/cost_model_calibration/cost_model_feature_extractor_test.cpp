#include <iostream>
#include <map>
#include <memory>
#include <optional>
#include <set>
#include <string>
#include <utility>
#include <vector>

#include "base_test.hpp"
#include "gtest/gtest.h"

#include "benchmark/cost_model_calibration/cost_model_feature_extractor.hpp"

namespace opossum {

class CostModelFeatureExtractorTest : public BaseTest {
 protected:
  void SetUp() override {}
};

TEST_F(CostModelFeatureExtractorTest, ExtractFeatures) {
  // set up some TableScanOperator

  auto result_json = CostModelFeatureExtractor::extract_features(op);
}

}  // namespace opossum
