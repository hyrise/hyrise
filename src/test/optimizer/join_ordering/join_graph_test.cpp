#include "gtest/gtest.h"

#include "logical_query_plan/mock_node.hpp"
#include "optimizer/join_ordering/join_graph.hpp"
#include "optimizer/join_ordering/join_plan_predicate.hpp"

namespace opossum {

using namespace std::string_literals;  // NOLINT

class JoinGraphTest : public ::testing::Test {
 public:
  static bool contains_predicate(const std::vector<std::shared_ptr<const AbstractJoinPlanPredicate>>& predicates,
                                 const std::shared_ptr<const AbstractJoinPlanPredicate>& predicate) {
    for (const auto& predicate2 : predicates) {
      if (predicate2->type() != predicate->type()) continue;

      switch (predicate->type()) {
        case JoinPlanPredicateType::Atomic: {
          const auto atomic_predicate = std::static_pointer_cast<const JoinPlanAtomicPredicate>(predicate);
          const auto atomic_predicate2 = std::static_pointer_cast<const JoinPlanAtomicPredicate>(predicate2);
          if (*atomic_predicate == *atomic_predicate2) return true;
        } break;

        case JoinPlanPredicateType::LogicalOperator: {
          const auto logical_predicate = std::static_pointer_cast<const JoinPlanLogicalPredicate>(predicate);
          const auto logical_predicate2 = std::static_pointer_cast<const JoinPlanLogicalPredicate>(predicate2);
          if (*logical_predicate == *logical_predicate2) return true;
        } break;
      }
    }

    return false;
  }
};

TEST_F(JoinGraphTest, SingleVertexNoPredicates) {
  /**
   * Basic Test that makes sure no predicates are returned from a JoinGraph with a single vertex and no predicates
   */

  const auto vertex = std::make_shared<MockNode>(MockNode::ColumnDefinitions{{DataType::Int, "a"}});

  const auto join_graph = JoinGraph::from_predicates({vertex}, {}, {});

  JoinVertexSet vertex_set{1};
  vertex_set.set(0);

  const auto found_predicates = join_graph->find_predicates(vertex_set);

  EXPECT_EQ(found_predicates.size(), 0u);
}

TEST_F(JoinGraphTest, SingleVertexMultiplePredicates) {
  /**
   * Test that all predicates from a single vertex are returned when looking for all predicates of that vertex
   */

  const auto vertex =
      std::make_shared<MockNode>(MockNode::ColumnDefinitions{{DataType::Int, "a"}, {DataType::Int, "b"}});
  const auto vertex_a = vertex->get_column("a"s);
  const auto vertex_b = vertex->get_column("b"s);
  const auto predicate_a = std::make_shared<JoinPlanAtomicPredicate>(vertex_a, PredicateCondition::Equals, 5);
  const auto predicate_b = std::make_shared<JoinPlanAtomicPredicate>(vertex_a, PredicateCondition::Equals, vertex_b);
  const auto predicate_c =
      std::make_shared<JoinPlanLogicalPredicate>(predicate_a, JoinPlanPredicateLogicalOperator::Or, predicate_b);

  const auto join_graph = JoinGraph::from_predicates({vertex}, {}, {predicate_a, predicate_b, predicate_c});

  const auto vertex_set = JoinVertexSet{1, 1};
  const auto found_predicates = join_graph->find_predicates(vertex_set);

  EXPECT_EQ(found_predicates.size(), 3u);
  EXPECT_TRUE(contains_predicate(found_predicates, predicate_a));
  EXPECT_TRUE(contains_predicate(found_predicates, predicate_b));
  EXPECT_TRUE(contains_predicate(found_predicates, predicate_c));
}

TEST_F(JoinGraphTest, MultipleVerticesMultiplePredicates) {
  /**
   * In a JoinGraph with 3 vertices and predicates spanning over more than one vertex, test that both versions of
   * JoinGraph::find_predicates() return the correct predicates
   */

  const auto vertex_a = std::make_shared<MockNode>(MockNode::ColumnDefinitions{{DataType::Int, "x"}});
  const auto vertex_b = std::make_shared<MockNode>(MockNode::ColumnDefinitions{{DataType::Int, "x"}});
  const auto vertex_c = std::make_shared<MockNode>(MockNode::ColumnDefinitions{{DataType::Int, "x"}});

  const auto vertex_a_x = vertex_a->get_column("x"s);
  const auto vertex_b_x = vertex_b->get_column("x"s);
  const auto vertex_c_x = vertex_c->get_column("x"s);

  const auto predicate_a = std::make_shared<JoinPlanAtomicPredicate>(vertex_a_x, PredicateCondition::Equals, 5);
  const auto predicate_b =
      std::make_shared<JoinPlanAtomicPredicate>(vertex_b_x, PredicateCondition::Equals, vertex_c_x);
  const auto predicate_c =
      std::make_shared<JoinPlanLogicalPredicate>(predicate_a, JoinPlanPredicateLogicalOperator::Or, predicate_b);
  const auto predicate_d =
      std::make_shared<JoinPlanLogicalPredicate>(predicate_a, JoinPlanPredicateLogicalOperator::And, predicate_b);

  const auto join_graph = JoinGraph::from_predicates({vertex_a, vertex_b, vertex_c}, {},
                                                     {predicate_a, predicate_b, predicate_c, predicate_d});

  const auto vertex_set_a = JoinVertexSet{3, 0b001};
  const auto vertex_set_b = JoinVertexSet{3, 0b010};
  const auto vertex_set_c = JoinVertexSet{3, 0b100};

  const auto found_predicates_a = join_graph->find_predicates(vertex_set_a);
  const auto found_predicates_b = join_graph->find_predicates(vertex_set_b);
  const auto found_predicates_c = join_graph->find_predicates(vertex_set_c);
  const auto found_predicates_abc_1 = join_graph->find_predicates(vertex_set_a, vertex_set_b | vertex_set_c);
  const auto found_predicates_abc_2 = join_graph->find_predicates(vertex_set_a | vertex_set_b, vertex_set_c);
  const auto found_predicates_abc_3 = join_graph->find_predicates(vertex_set_a | vertex_set_b | vertex_set_c);
  const auto found_predicates_bc_1 = join_graph->find_predicates(vertex_set_b | vertex_set_c);
  const auto found_predicates_bc_2 = join_graph->find_predicates(vertex_set_b, vertex_set_c);
  const auto found_predicates_ab_1 = join_graph->find_predicates(vertex_set_a | vertex_set_b);
  const auto found_predicates_ab_2 = join_graph->find_predicates(vertex_set_a, vertex_set_b);

  EXPECT_EQ(found_predicates_a.size(), 1u);
  EXPECT_TRUE(contains_predicate(found_predicates_a, predicate_a));

  EXPECT_TRUE(found_predicates_b.empty());

  EXPECT_TRUE(found_predicates_c.empty());

  EXPECT_EQ(found_predicates_abc_1.size(), 2u);
  EXPECT_TRUE(contains_predicate(found_predicates_abc_1, predicate_c));
  EXPECT_TRUE(contains_predicate(found_predicates_abc_1, predicate_d));

  EXPECT_EQ(found_predicates_abc_2.size(), 3u);
  EXPECT_TRUE(contains_predicate(found_predicates_abc_2, predicate_c));
  EXPECT_TRUE(contains_predicate(found_predicates_abc_2, predicate_d));
  EXPECT_TRUE(contains_predicate(found_predicates_abc_2, predicate_b));

  EXPECT_EQ(found_predicates_abc_3.size(), 2u);
  EXPECT_TRUE(contains_predicate(found_predicates_abc_3, predicate_c));
  EXPECT_TRUE(contains_predicate(found_predicates_abc_3, predicate_d));

  EXPECT_EQ(found_predicates_bc_1.size(), 1u);
  EXPECT_TRUE(contains_predicate(found_predicates_bc_1, predicate_b));

  EXPECT_EQ(found_predicates_bc_2.size(), 1u);
  EXPECT_TRUE(contains_predicate(found_predicates_bc_2, predicate_b));

  EXPECT_TRUE(found_predicates_ab_1.empty());

  EXPECT_TRUE(found_predicates_ab_2.empty());
}

TEST_F(JoinGraphTest, Print) {
  const auto vertex_a = std::make_shared<MockNode>(MockNode::ColumnDefinitions{{DataType::Int, "x"}});
  const auto vertex_b = std::make_shared<MockNode>(MockNode::ColumnDefinitions{{DataType::Int, "y"}});
  const auto vertex_c = std::make_shared<MockNode>(MockNode::ColumnDefinitions{{DataType::Int, "z"}});

  const auto vertex_a_x = vertex_a->get_column("x"s);
  const auto vertex_b_y = vertex_b->get_column("y"s);
  const auto vertex_c_z = vertex_c->get_column("z"s);

  const auto predicate_a = std::make_shared<JoinPlanAtomicPredicate>(vertex_a_x, PredicateCondition::Equals, 5);
  const auto predicate_b =
      std::make_shared<JoinPlanAtomicPredicate>(vertex_b_y, PredicateCondition::Equals, vertex_c_z);
  const auto predicate_c =
      std::make_shared<JoinPlanLogicalPredicate>(predicate_a, JoinPlanPredicateLogicalOperator::Or, predicate_b);

  const auto join_graph =
      JoinGraph::from_predicates({vertex_a, vertex_b, vertex_c}, {}, {predicate_a, predicate_b, predicate_c});

  std::stringstream stream;
  join_graph->print(stream);

  EXPECT_EQ(stream.str(), R"(==== Vertices ====
[MockTable]
[MockTable]
[MockTable]
===== Edges ======
Edge between 001, 1 predicates
x = 5
Edge between 110, 1 predicates
y = z
Edge between 111, 1 predicates
x = 5 OR y = z
)");
}

}  // namespace opossum
