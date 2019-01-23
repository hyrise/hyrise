#include <unordered_map>

//#include "gtest/gtest.h"

#include "base_test.hpp"

#include "cost_model/cost_model_adaptive.hpp"
#include "expression/expression_functional.hpp"
#include "logical_query_plan/mock_node.hpp"
#include "logical_query_plan/predicate_node.hpp"
#include "statistics/base_column_statistics.hpp"
#include "statistics/column_statistics.hpp"
#include "statistics/table_statistics.hpp"

using namespace opossum::expression_functional;  // NOLINT

namespace opossum {

class CostModelAdaptiveTest : public BaseTest {
 public:
  void SetUp() override {
    node_a = MockNode::make(MockNode::ColumnDefinitions{{DataType::Int, "a"}, {DataType::String, "b"}}, "a");

    // Just some dummy statistics
    const auto int_column_statistics = std::make_shared<ColumnStatistics<int32_t>>(0.0f, 10.0f, 1, 50);
    const auto string_column_statistics = std::make_shared<ColumnStatistics<std::string>>(0.0f, 10.0f, "a", "z");
    const auto table_statistics = std::make_shared<TableStatistics>(
        TableType::Data, 20,
        std::vector<std::shared_ptr<const BaseColumnStatistics>>{int_column_statistics, string_column_statistics});
    node_a->set_statistics(table_statistics);

    a_a = node_a->get_column("a");
    a_b = node_a->get_column("b");
  }

  std::shared_ptr<MockNode> node_a;
  LQPColumnReference a_a;
  LQPColumnReference a_b;
};

TEST_F(CostModelAdaptiveTest, CostPredicate) {
  std::unordered_map<const ModelGroup, const std::unordered_map<std::string, float>, ModelGroupHash> coefficients{
      {{OperatorType::TableScan, DataType::Int, false, false}, {{"left_input_row_count", 3}, {"output_row_count", 4}}},
      {{OperatorType::TableScan, DataType::String, false, false},
       {{"left_input_row_count", 8}, {"output_row_count", 2}}},
  };

  class MockFeatureExtractor : public AbstractFeatureExtractor {
   public:
    explicit MockFeatureExtractor(const CostModelFeatures& features) : _features(features) {}

   protected:
    const CostModelFeatures extract_features(const std::shared_ptr<const AbstractLQPNode>& node) const override {
      return _features;
    }

    const CostModelFeatures _features;
  };

  CostModelAdaptive cost_model(coefficients, std::make_shared<MockFeatureExtractor>(CostModelFeatures{}));

  auto predicate_node = PredicateNode::make(equals_(a_a, 10), node_a);
  EXPECT_EQ(cost_model.estimate_plan_cost(predicate_node), Cost{0});

  predicate_node = PredicateNode::make(equals_(a_b, "a"), node_a);
  EXPECT_EQ(cost_model.estimate_plan_cost(predicate_node), Cost{0});
}

}  // namespace opossum
