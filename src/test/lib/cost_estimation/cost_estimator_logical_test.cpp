#include "base_test.hpp"
#include "cost_estimation/cost_estimator_logical.hpp"
#include "expression/expression_functional.hpp"
#include "logical_query_plan/join_node.hpp"
#include "logical_query_plan/mock_node.hpp"
#include "logical_query_plan/predicate_node.hpp"
#include "logical_query_plan/sort_node.hpp"
#include "logical_query_plan/stored_table_node.hpp"
#include "logical_query_plan/union_node.hpp"

namespace hyrise {

using namespace expression_functional;  // NOLINT(build/namespaces)

class CostEstimatorLogicalTest : public BaseTest {
 public:
  void SetUp() override {
    cost_estimator = std::make_shared<CostEstimatorLogical>(std::make_shared<CardinalityEstimator>());

    node_a = create_mock_node_with_statistics(
        MockNode::ColumnDefinitions{{DataType::Int, "a"}, {DataType::Int, "b"}, {DataType::Int, "c"}}, 100,
        {GenericHistogram<int32_t>::with_single_bin(10, 100, 100, 20),
         GenericHistogram<int32_t>::with_single_bin(50, 60, 100, 5),
         GenericHistogram<int32_t>::with_single_bin(110, 1100, 100, 2)});

    node_b = create_mock_node_with_statistics(
        MockNode::ColumnDefinitions{{DataType::Int, "a"}, {DataType::Int, "b"}, {DataType::Int, "c"}}, 50,
        {GenericHistogram<int32_t>::with_single_bin(10, 100, 100, 20),
         GenericHistogram<int32_t>::with_single_bin(50, 60, 100, 5),
         GenericHistogram<int32_t>::with_single_bin(110, 1100, 100, 2)});

    a_a = node_a->get_column("a");
    a_b = node_a->get_column("b");
    b_a = node_b->get_column("a");
    b_b = node_b->get_column("b");
  }

  std::shared_ptr<CostEstimatorLogical> cost_estimator;
  std::shared_ptr<MockNode> node_a;
  std::shared_ptr<MockNode> node_b;
  std::shared_ptr<LQPColumnExpression> a_a;
  std::shared_ptr<LQPColumnExpression> a_b;
  std::shared_ptr<LQPColumnExpression> b_a;
  std::shared_ptr<LQPColumnExpression> b_b;
};

TEST_F(CostEstimatorLogicalTest, StoredTableNode) {
  // Does not actually process data, so we pretend it's for free.
  Hyrise::get().storage_manager.add_table("table_a", load_table("resources/test_data/tbl/int_float.tbl"));
  const auto stored_table_node = StoredTableNode::make("table_a");
  EXPECT_FLOAT_EQ(cost_estimator->estimate_node_cost(stored_table_node), 0);
}

TEST_F(CostEstimatorLogicalTest, SortNode) {
  // Sorting is in n * log(n). Plus output writing.
  const auto sort_node = SortNode::make(expression_vector(a_a), std::vector{SortMode::Ascending}, node_a);

  const auto expected_cost = 100.0f * std::log(100.0f) + 100.0f;
  EXPECT_FLOAT_EQ(cost_estimator->estimate_node_cost(sort_node), expected_cost);
}

TEST_F(CostEstimatorLogicalTest, UnionAll) {
  // Does not actually process data, so we pretend it's for free.
  const auto union_node = UnionNode::make(SetOperationMode::All, node_a, node_b);
  EXPECT_FLOAT_EQ(cost_estimator->estimate_node_cost(union_node), 0);
}

TEST_F(CostEstimatorLogicalTest, UnionPositions) {
  // We need to sort both inputs' PosLists (n * log(n)) and write the output.
  const auto union_node = UnionNode::make(SetOperationMode::Positions, node_a, node_b);

  const auto output_cardinality = cost_estimator->cardinality_estimator->estimate_cardinality(union_node);
  const auto expected_cost = 100.0f * std::log(100.0f) + 50.0f * std::log(50.0f) + output_cardinality;
  EXPECT_FLOAT_EQ(cost_estimator->estimate_node_cost(union_node), expected_cost);
}

TEST_F(CostEstimatorLogicalTest, UnionUnique) {
  // Not implemented yet.
  const auto union_node = UnionNode::make(SetOperationMode::Unique, node_a, node_b);
  EXPECT_THROW(cost_estimator->estimate_node_cost(union_node), std::logic_error);
}

TEST_F(CostEstimatorLogicalTest, PredicatedJoins) {
  // Independent of the join mode and the join predicate, we must read both inputs and write the output.
  for (const auto& predicate :
       {expression_vector(equals_(a_a, b_a)), expression_vector(equals_(a_a, b_a), not_equals_(a_b, b_b))}) {
    for (const auto join_mode : {JoinMode::Inner, JoinMode::Semi, JoinMode::FullOuter, JoinMode::Left, JoinMode::Right,
                                 JoinMode::AntiNullAsFalse, JoinMode::AntiNullAsTrue}) {
      const auto join_node = JoinNode::make(join_mode, predicate, node_a, node_b);

      const auto output_cardinality = cost_estimator->cardinality_estimator->estimate_cardinality(join_node);
      const auto expected_cost = 100.0f + 50.0f + output_cardinality;
      EXPECT_FLOAT_EQ(cost_estimator->estimate_node_cost(join_node), expected_cost);
    }
  }
}

TEST_F(CostEstimatorLogicalTest, CrossJoin) {
  // Independent of the join mode and the join predicate, we must read both inputs and write the output.
  const auto join_node = JoinNode::make(JoinMode::Cross, node_a, node_b);

  const auto output_cardinality = cost_estimator->cardinality_estimator->estimate_cardinality(join_node);
  const auto expected_cost = 100.0f + 50.0f + output_cardinality;
  EXPECT_FLOAT_EQ(cost_estimator->estimate_node_cost(join_node), expected_cost);
}

TEST_F(CostEstimatorLogicalTest, SingleColumnPredicate) {
  // We must read the input and write the output.
  for (const auto& predicate :
       expression_vector(equals_(a_a, 20), not_equals_(a_a, 20), greater_than_(a_a, 20),
                         between_inclusive_(a_a, 20, 100), is_null_(a_a), equals_(a_a, lqp_subquery_(node_b)))) {
    const auto predicate_node = PredicateNode::make(predicate, node_a);
    const auto output_cardinality = cost_estimator->cardinality_estimator->estimate_cardinality(predicate_node);
    const auto expected_cost = 100.0f + output_cardinality;
    EXPECT_FLOAT_EQ(cost_estimator->estimate_node_cost(predicate_node), expected_cost);
  }
}

TEST_F(CostEstimatorLogicalTest, MultiColumnPredicate) {
  // For column vs. column predicates, we must read both columns of the input and write the output.
  const auto predicate_node = PredicateNode::make(less_than_(a_a, a_b), node_a);
  const auto output_cardinality = cost_estimator->cardinality_estimator->estimate_cardinality(predicate_node);
  const auto expected_cost = 2 * 100.0f + output_cardinality;
  EXPECT_FLOAT_EQ(cost_estimator->estimate_node_cost(predicate_node), expected_cost);
}

TEST_F(CostEstimatorLogicalTest, ComplexPredicate) {
  // We must read all required columns of the input and write the output.
  const auto predicate_node =
      PredicateNode::make(or_(equals_(a_a, 10), and_(less_than_(a_b, 55), greater_than_(a_a, 50))), node_a);
  const auto output_cardinality = cost_estimator->cardinality_estimator->estimate_cardinality(predicate_node);
  const auto expected_cost = 3 * 100.0f + output_cardinality;
  EXPECT_FLOAT_EQ(cost_estimator->estimate_node_cost(predicate_node), expected_cost);
}

TEST_F(CostEstimatorLogicalTest, PredicateWithCorrelatedSubquery) {
  // We must execute the correlated subquery for each row of the input. Also, each correlated parameter adds
  // comparison overhead. Thus, we add an additional factor (i) for the correlated subquery and (ii) for each
  // correlated parameter. Finally, we must write the output.
  const auto predicate_node =
      PredicateNode::make(less_than_(a_a, lqp_subquery_(node_b, std::make_pair(ParameterID{0}, a_b))), node_a);
  const auto output_cardinality = cost_estimator->cardinality_estimator->estimate_cardinality(predicate_node);
  const auto expected_cost = 3 * 100.0f + output_cardinality;
  EXPECT_FLOAT_EQ(cost_estimator->estimate_node_cost(predicate_node), expected_cost);
}

TEST_F(CostEstimatorLogicalTest, CardinalityCaching) {
  // Enable cardinality estimation caching.
  const auto& cardinality_estimator = cost_estimator->cardinality_estimator;
  cardinality_estimator->guarantee_bottom_up_construction();
  const auto& cardinality_cache = cardinality_estimator->cardinality_estimation_cache.statistics_by_lqp;
  ASSERT_TRUE(cardinality_cache);
  EXPECT_TRUE(cardinality_cache->empty());

  // Estimate the cost of a node with cardinality caching enabled.
  const auto predicate_node_1 = PredicateNode::make(greater_than_(a_a, 50), node_a);
  cost_estimator->estimate_node_cost(predicate_node_1);
  EXPECT_EQ(cardinality_cache->size(), 2);
  EXPECT_TRUE(cardinality_cache->contains(node_a));
  EXPECT_TRUE(cardinality_cache->contains(predicate_node_1));

  // Estimate the cost of a node with cardinality caching disabled.
  const auto predicate_node_2 = PredicateNode::make(less_than_(a_b, 55), node_a);
  cost_estimator->estimate_node_cost(predicate_node_2, false);
  EXPECT_EQ(cardinality_cache->size(), 2);
  EXPECT_FALSE(cardinality_cache->contains(predicate_node_2));
}

}  // namespace hyrise
