#include "gtest/gtest.h"

#include "expression/expression_functional.hpp"
#include "logical_query_plan/abstract_lqp_node.hpp"
#include "logical_query_plan/aggregate_node.hpp"
#include "logical_query_plan/create_view_node.hpp"
#include "logical_query_plan/delete_node.hpp"
#include "logical_query_plan/drop_view_node.hpp"
#include "logical_query_plan/dummy_table_node.hpp"
#include "logical_query_plan/insert_node.hpp"
#include "logical_query_plan/join_node.hpp"
#include "logical_query_plan/limit_node.hpp"
#include "logical_query_plan/logical_plan_root_node.hpp"
#include "logical_query_plan/lqp_utils.hpp"
#include "logical_query_plan/mock_node.hpp"
#include "logical_query_plan/predicate_node.hpp"
#include "logical_query_plan/projection_node.hpp"
#include "logical_query_plan/show_columns_node.hpp"
#include "logical_query_plan/show_tables_node.hpp"
#include "logical_query_plan/sort_node.hpp"
#include "logical_query_plan/stored_table_node.hpp"
#include "logical_query_plan/union_node.hpp"
#include "logical_query_plan/update_node.hpp"
#include "logical_query_plan/validate_node.hpp"
#include "statistics/column_statistics.hpp"
#include "statistics/table_statistics.hpp"
#include "storage/storage_manager.hpp"
#include "utils/load_table.hpp"

#include "testing_assert.hpp"

using namespace opossum::expression_functional;  // NOLINT

namespace opossum {

class LQPFindSubplanMismatchTest : public ::testing::Test {
 protected:
  struct QueryNodes {
    std::shared_ptr<ValidateNode> validate_node;
    std::shared_ptr<StoredTableNode> stored_table_node_a;
    std::shared_ptr<MockNode> mock_node_a;
    std::shared_ptr<MockNode> mock_node_b;
    std::shared_ptr<PredicateNode> predicate_node_a;
    std::shared_ptr<PredicateNode> predicate_node_b;
    std::shared_ptr<UnionNode> union_node;
    std::shared_ptr<LimitNode> limit_node;
    std::shared_ptr<JoinNode> join_node;
    std::shared_ptr<AggregateNode> aggregate_node;
    std::shared_ptr<SortNode> sort_node;
    std::shared_ptr<ProjectionNode> projection_node;

    LQPColumnReference table_a_a;
    LQPColumnReference table_b_a;
    LQPColumnReference table_c_a;
    LQPColumnReference table_c_b;
  };

  void SetUp() override {
    StorageManager::get().add_table("table_a", load_table("src/test/tables/int_int.tbl", 2));

    _init_query_nodes(_query_nodes_lhs);
    _init_query_nodes(_query_nodes_rhs);
  }

  void TearDown() override { StorageManager::get().reset(); }

  void _init_query_nodes(QueryNodes& query_nodes) const {
    query_nodes.stored_table_node_a = StoredTableNode::make("table_a");
    query_nodes.validate_node = ValidateNode::make();
    query_nodes.mock_node_a = MockNode::make(MockNode::ColumnDefinitions{{DataType::Int, "a"}});
    query_nodes.mock_node_b = MockNode::make(MockNode::ColumnDefinitions{{DataType::Int, "b"}, {DataType::Int, "c"}});

    query_nodes.table_a_a = LQPColumnReference{query_nodes.stored_table_node_a, ColumnID{0}};
    query_nodes.table_b_a = LQPColumnReference{query_nodes.mock_node_a, ColumnID{0}};
    query_nodes.table_c_a = LQPColumnReference{query_nodes.mock_node_b, ColumnID{0}};
    query_nodes.table_c_b = LQPColumnReference{query_nodes.mock_node_b, ColumnID{1}};

    query_nodes.predicate_node_a = PredicateNode::make(less_than_(query_nodes.table_a_a, 41));
    query_nodes.predicate_node_b = PredicateNode::make(between(query_nodes.table_a_a, 42, 45));
    query_nodes.union_node = UnionNode::make(UnionMode::Positions);
    query_nodes.limit_node = LimitNode::make(to_expression(10));
    query_nodes.join_node = JoinNode::make(JoinMode::Inner, equals_(query_nodes.table_a_a, query_nodes.table_c_b));

    query_nodes.aggregate_node =
        AggregateNode::make(expression_vector(query_nodes.table_c_b), expression_vector(sum_(query_nodes.table_c_a)));

    query_nodes.sort_node =
        SortNode::make(expression_vector(query_nodes.table_c_b), std::vector<OrderByMode>{OrderByMode::Ascending});
    query_nodes.projection_node = ProjectionNode::make(expression_vector(query_nodes.table_a_a));
  }

  std::shared_ptr<AbstractLQPNode> _build_query_lqp(QueryNodes& query_nodes) {
    query_nodes.validate_node->set_left_input(query_nodes.stored_table_node_a);
    query_nodes.predicate_node_a->set_left_input(query_nodes.validate_node);
    query_nodes.predicate_node_b->set_left_input(query_nodes.stored_table_node_a);
    query_nodes.union_node->set_left_input(query_nodes.predicate_node_a);
    query_nodes.union_node->set_right_input(query_nodes.predicate_node_b);
    query_nodes.limit_node->set_left_input(query_nodes.union_node);
    query_nodes.join_node->set_left_input(query_nodes.limit_node);
    query_nodes.join_node->set_right_input(query_nodes.sort_node);
    query_nodes.sort_node->set_left_input(query_nodes.aggregate_node);
    query_nodes.aggregate_node->set_left_input(query_nodes.mock_node_b);
    query_nodes.projection_node->set_left_input(query_nodes.join_node);

    return query_nodes.projection_node;
  }

  void _build_query_lqps() {
    _query_lqp_lhs = _build_query_lqp(_query_nodes_lhs);
    _query_lqp_rhs = _build_query_lqp(_query_nodes_rhs);
  }

  QueryNodes _query_nodes_lhs, _query_nodes_rhs;
  std::shared_ptr<AbstractLQPNode> _query_lqp_lhs, _query_lqp_rhs;
};

TEST_F(LQPFindSubplanMismatchTest, EqualsTest) {
  _build_query_lqps();
  EXPECT_LQP_EQ(_query_lqp_lhs, _query_lqp_rhs);

  // Just test two more arbitrary subplans
  EXPECT_LQP_EQ(_query_nodes_lhs.predicate_node_a, _query_nodes_rhs.predicate_node_a);
  EXPECT_LQP_EQ(_query_nodes_lhs.join_node, _query_nodes_rhs.join_node);
}

TEST_F(LQPFindSubplanMismatchTest, SubplanMismatch) {
  _query_nodes_rhs.predicate_node_b = PredicateNode::make(between(_query_nodes_rhs.table_a_a, 42, 46));

  _build_query_lqps();

  auto mismatch = lqp_find_subplan_mismatch(_query_lqp_lhs, _query_lqp_rhs);
  ASSERT_TRUE(mismatch);
  EXPECT_EQ(mismatch->first, _query_nodes_lhs.predicate_node_b);
  EXPECT_EQ(mismatch->second, _query_nodes_rhs.predicate_node_b);
}

TEST_F(LQPFindSubplanMismatchTest, AdditionalNode) {
  const auto additional_predicate_node = PredicateNode::make(between(_query_nodes_rhs.table_a_a, 42, 45));

  _build_query_lqps();

  _query_nodes_rhs.predicate_node_b->set_left_input(additional_predicate_node);
  additional_predicate_node->set_left_input(_query_nodes_rhs.stored_table_node_a);

  auto mismatch = lqp_find_subplan_mismatch(_query_lqp_lhs, _query_lqp_rhs);
  ASSERT_TRUE(mismatch);
  EXPECT_EQ(mismatch->first, _query_nodes_lhs.stored_table_node_a);
  EXPECT_EQ(mismatch->second, additional_predicate_node);
}

TEST_F(LQPFindSubplanMismatchTest, TypeMismatch) {
  const auto first_node = PredicateNode::make(between(_query_nodes_rhs.table_a_a, int32_t{42}, int32_t{45}));
  const auto second_node = PredicateNode::make(between(_query_nodes_rhs.table_a_a, int64_t{42}, int64_t{45}));

  first_node->set_left_input(_query_nodes_rhs.stored_table_node_a);
  second_node->set_left_input(_query_nodes_rhs.stored_table_node_a);

  auto mismatch = lqp_find_subplan_mismatch(first_node, second_node);
  ASSERT_TRUE(mismatch);
}

}  // namespace opossum
