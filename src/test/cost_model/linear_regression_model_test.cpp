#include <unordered_map>

#include "base_test.hpp"

#include "cost_model/linear_regression_model.hpp"
#include "expression/expression_functional.hpp"

using namespace opossum::expression_functional;  // NOLINT

namespace opossum {

class LinearRegressionModelTest : public BaseTest {
 public:
  void SetUp() override {}
};

TEST_F(LinearRegressionModelTest, Predict) {
  const auto model = LinearRegressionModel({
      {"a", 10},
      {"b", 2},
      {"c", -5},
  });

  const std::unordered_map<std::string, float> features{
      {"a", 2},
      {"b", 2},
      {"c", 2},
  };

  EXPECT_EQ(model.predict(features), Cost{14});
}

TEST_F(LinearRegressionModelTest, MissingFeature) {
  const auto model = LinearRegressionModel({
      {"a", 10},
      {"b", 2},
      {"c", -5},
  });

  const std::unordered_map<std::string, float> features{
      {"a", 2},
      {"b", 2},
  };

  EXPECT_THROW(model.predict(features), std::logic_error);
}

TEST_F(LinearRegressionModelTest, AdditionalFeature) {
  const auto model = LinearRegressionModel({
      {"a", 10},
      {"b", 2},
  });

  const std::unordered_map<std::string, float> features{
      {"a", 2},
      {"b", 2},
      {"c", 2},
  };

  EXPECT_EQ(model.predict(features), Cost{24});
}

}  // namespace opossum
