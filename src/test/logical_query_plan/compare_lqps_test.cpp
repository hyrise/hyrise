#include "gtest/gtest.h"

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
#include "optimizer/column_statistics.hpp"
#include "optimizer/table_statistics.hpp"
#include "storage/storage_manager.hpp"
#include "utils/load_table.hpp"

#include "testing_assert.hpp"

namespace {

using namespace opossum;

struct QueryNodes {
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
};

}

namespace opossum {

class CompareLQPsTest : public ::testing::Test {
 protected:
  void SetUp() override {
    StorageManager::get().add_table("table_a", load_table("src/test/tables/int_int.tbl", 2));

    _init_query_nodes(_query_nodes_lhs);
    _init_query_nodes(_query_nodes_rhs);
  }

  void _init_query_nodes(QueryNodes &query_nodes) const {
    query_nodes.stored_table_node_a = std::make_shared<StoredTableNode>("table_a");
    query_nodes.mock_node_a = std::make_shared<MockNode>(MockNode::ColumnDefinitions{{DataType::Int, "a"}});
    query_nodes.mock_node_b = std::make_shared<MockNode>(MockNode::ColumnDefinitions{{DataType::Int, "b"}, {DataType::Int, "c"}});

    const auto table_a_a = LQPColumnReference{query_nodes.stored_table_node_a, ColumnID{0}};
    const auto table_b_a = LQPColumnReference{query_nodes.mock_node_a, ColumnID{0}};
    const auto table_c_a = LQPColumnReference{query_nodes.mock_node_b, ColumnID{0}};
    const auto table_c_b = LQPColumnReference{query_nodes.mock_node_b, ColumnID{1}};

    query_nodes.predicate_node_a = std::make_shared<PredicateNode>(table_a_a, PredicateCondition::LessThan, 41);
    query_nodes.predicate_node_b = std::make_shared<PredicateNode>(table_a_a, PredicateCondition::GreaterThan, 42);
    query_nodes.union_node = std::make_shared<UnionNode>(UnionMode::Positions);
    query_nodes.limit_node = std::make_shared<LimitNode>(10);
    query_nodes.join_node = std::make_shared<JoinNode>(JoinMode::Inner, LQPColumnReferencePair{table_a_a, table_c_b}, PredicateCondition::Equals);

    std::vector<std::shared_ptr<LQPExpression>> aggregates{LQPExpression::create_aggregate_function(AggregateFunction::Sum, LQPExpression::create_columns({table_c_a}))};
    std::vector<LQPColumnReference> groupby_column_references{table_c_b};
    query_nodes.aggregate_node = std::make_shared<AggregateNode>(aggregates, groupby_column_references);

    query_nodes.sort_node = std::make_shared<SortNode>(OrderByDefinitions{{table_c_b, OrderByMode::Ascending}});
    query_nodes.projection_node = std::make_shared<ProjectionNode>(std::vector<std::shared_ptr<LQPExpression>>{LQPExpression::create_column(table_a_a)});
  }

  std::shared_ptr<AbstractLQPNode> _build_query_lqp(QueryNodes &query_nodes) {
    query_nodes.predicate_node_a->set_left_child(query_nodes.stored_table_node_a);
    query_nodes.predicate_node_b->set_left_child(query_nodes.stored_table_node_a);
    query_nodes.union_node->set_left_child(query_nodes.predicate_node_a);
    query_nodes.union_node->set_right_child(query_nodes.predicate_node_b);
    query_nodes.limit_node->set_left_child(query_nodes.union_node);
    query_nodes.join_node->set_left_child(query_nodes.limit_node);
    query_nodes.join_node->set_right_child(query_nodes.sort_node);
    query_nodes.sort_node->set_left_child(query_nodes.aggregate_node);
    query_nodes.aggregate_node->set_left_child(query_nodes.mock_node_b);
    query_nodes.projection_node->set_left_child(query_nodes.join_node);
    return query_nodes.projection_node;
  }

  void _build_query_lqps() {
    _query_lqp_lhs = _build_query_lqp(_query_nodes_lhs);
    _query_lqp_rhs = _build_query_lqp(_query_nodes_rhs);
  }

  QueryNodes _query_nodes_lhs, _query_nodes_rhs;
  std::shared_ptr<AbstractLQPNode> _query_lqp_lhs, _query_lqp_rhs;
};

TEST_F(CompareLQPsTest, EqualsTest) {
  _build_query_lqps();
  EXPECT_LQP_SEMANTICALLY_EQ(_query_lqp_lhs, _query_lqp_rhs);
}

}  // namespace opossum