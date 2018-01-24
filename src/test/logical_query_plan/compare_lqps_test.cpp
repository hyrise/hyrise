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
#include "storage/storage_manager.hpp"
#include "utils/load_table.hpp"

#include "testing_assert.hpp"

namespace {

using namespace opossum;

struct QueryNodes {
  std::shared_ptr<StoredTableNode> stored_table_node_a;
  std::shared_ptr<StoredTableNode> stored_table_node_b;
  std::shared_ptr<PredicateNode> predicate_node_a;
  std::shared_ptr<PredicateNode> predicate_node_b;
  std::shared_ptr<UnionNode> union_node;
};

}

namespace opossum {

class CompareLQPsTest : public ::testing::Test {
 protected:
  void SetUp() override {
    StorageManager::get().add_table("table_a", load_table("src/test/tables/int_int.tbl", 2));
    StorageManager::get().add_table("table_b", load_table("src/test/tables/int_int.tbl", 2));

    _init_query_nodes(_query_nodes_lhs);
    _init_query_nodes(_query_nodes_rhs);
  }

  void _init_query_nodes(QueryNodes &query_nodes) const {
    query_nodes.stored_table_node_a = std::make_shared<StoredTableNode>("table_a");
    query_nodes.stored_table_node_b = std::make_shared<StoredTableNode>("table_b");

    const auto table_a_a = LQPColumnReference{query_nodes.stored_table_node_a, ColumnID{0}};
    const auto table_b_a = LQPColumnReference{query_nodes.stored_table_node_b, ColumnID{0}};

    query_nodes.predicate_node_a = std::make_shared<PredicateNode>(table_a_a, ScanType::Equals, 42);
    query_nodes.predicate_node_b = std::make_shared<PredicateNode>(table_b_a, ScanType::GreaterThan, 42);
    query_nodes.union_node = std::make_shared<UnionNode>(UnionMode::Positions);
  }

  std::shared_ptr<AbstractLQPNode> _build_query_lqp(QueryNodes &query_nodes) {
    query_nodes.predicate_node_a->set_left_child(query_nodes.stored_table_node_a);
    query_nodes.predicate_node_b->set_left_child(query_nodes.stored_table_node_b);
    query_nodes.union_node->set_left_child(query_nodes.predicate_node_a);
    query_nodes.union_node->set_right_child(query_nodes.predicate_node_b);
    return query_nodes.union_node;
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