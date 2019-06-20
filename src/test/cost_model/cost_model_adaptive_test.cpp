#include <unordered_map>

//#include "gtest/gtest.h"

#include "base_test.hpp"

#include "cost_estimation/cost_estimator_adaptive.hpp"
#include "expression/expression_functional.hpp"
#include "logical_query_plan/mock_node.hpp"
#include "logical_query_plan/predicate_node.hpp"
#include "statistics/attribute_statistics.hpp"
#include "statistics/base_attribute_statistics.hpp"
#include "statistics/table_statistics.hpp"

using namespace opossum::expression_functional;  // NOLINT

namespace opossum {

// TODO(anyone): disabled since we've seen too many changes with the histogram commit.

// class CostEstimatorAdaptiveTest : public BaseTest {
//  public:
//   void SetUp() override {
//     node_a = MockNode::make(MockNode::ColumnDefinitions{{DataType::Int, "a"}, {DataType::String, "b"}}, "a");

//     // Just some dummy statistics
//     const auto int_attribute_statistics = std::make_shared<AttributeStatistics<int32_t>>(0.0f, 10.0f, 1, 50);
//     const auto string_attribute_statistics = std::make_shared<AttributeStatistics<pmr_string>>(0.0f, 10.0f, "a", "z");
//     const auto table_statistics = std::make_shared<TableStatistics>(std::vector<std::shared_ptr<const BaseAttributeStatistics>>{int_attribute_statistics, string_attribute_statistics}, 20);
//     node_a->set_statistics(table_statistics);

//     a_a = node_a->get_column("a");
//     a_b = node_a->get_column("b");
//   }

//   std::shared_ptr<MockNode> node_a;
//   LQPColumnReference a_a;
//   LQPColumnReference a_b;
// };

// TEST_F(CostEstimatorAdaptiveTest, CostPredicate) {
//   std::unordered_map<const ModelGroup, const std::unordered_map<std::string, float>, ModelGroupHash> coefficients{
//       {{OperatorType::TableScan, DataType::Int, false, false}, {{"left_input_row_count", 3}, {"output_row_count", 4}}},
//       {{OperatorType::TableScan, DataType::String, false, false},
//        {{"left_input_row_count", 8}, {"output_row_count", 2}}},
//   };

//   class MockFeatureExtractor : public AbstractFeatureExtractor {
//    public:
//     explicit MockFeatureExtractor(const CostModelFeatures& features) : _features(features) {}

//    protected:
//     const CostModelFeatures extract_features(const std::shared_ptr<const AbstractLQPNode>& node) const override {
//       return _features;
//     }

//     const CostModelFeatures _features;
//   };

//   CostEstimatorAdaptive cost_model(coefficients, std::make_shared<MockFeatureExtractor>(CostModelFeatures{}));

//   auto predicate_node = PredicateNode::make(equals_(a_a, 10), node_a);
//   EXPECT_EQ(cost_model.estimate_plan_cost(predicate_node), Cost{0});

//   predicate_node = PredicateNode::make(equals_(a_b, "a"), node_a);
//   EXPECT_EQ(cost_model.estimate_plan_cost(predicate_node), Cost{0});
// }

}  // namespace opossum
