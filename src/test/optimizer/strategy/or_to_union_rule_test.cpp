#include "gtest/gtest.h"

#include "strategy_base_test.hpp"
#include "testing_assert.hpp"

//#include "expression/expression_functional.hpp"
//#include "expression/lqp_column_expression.hpp"
//#include "logical_query_plan/abstract_lqp_node.hpp"
//#include "logical_query_plan/aggregate_node.hpp"
//#include "logical_query_plan/join_node.hpp"
//#include "logical_query_plan/limit_node.hpp"
//#include "logical_query_plan/mock_node.hpp"
//#include "logical_query_plan/predicate_node.hpp"
//#include "logical_query_plan/projection_node.hpp"
//#include "logical_query_plan/sort_node.hpp"
//#include "logical_query_plan/stored_table_node.hpp"
//#include "logical_query_plan/union_node.hpp"
//#include "logical_query_plan/validate_node.hpp"
//#include "optimizer/strategy/subquery_to_join_rule.hpp"
#include "optimizer/strategy/or_to_union_rule.hpp"

using namespace opossum::expression_functional;  // NOLINT

namespace opossum {

class SubqueryToJoinRuleTest : public StrategyBaseTest {
 public:
  void SetUp() override {
    node_a = MockNode::make(
        MockNode::ColumnDefinitions{{DataType::Int, "a"}, {DataType::Int, "b"}, {DataType::Int, "c"}}, "a");
    a_a = node_a->get_column("a");
    a_b = node_a->get_column("b");
    a_c = node_a->get_column("c");
    a_a_expression = to_expression(a_a);
    a_b_expression = to_expression(a_b);
    a_c_expression = to_expression(a_c);

    node_b = MockNode::make(MockNode::ColumnDefinitions{{DataType::Int, "a"}, {DataType::Int, "b"}}, "b");
    b_a = node_b->get_column("a");
    b_b = node_b->get_column("b");

    node_c = MockNode::make(
        MockNode::ColumnDefinitions{{DataType::Int, "a"}, {DataType::Int, "b"}, {DataType::Int, "c"}}, "c");
    c_a = node_c->get_column("a");

    node_d = MockNode::make(
        MockNode::ColumnDefinitions{{DataType::Int, "a"}, {DataType::Int, "b"}, {DataType::Int, "c"}}, "d");
    d_a = node_d->get_column("a");
    d_b = node_d->get_column("b");
    d_c = node_d->get_column("c");

    node_e = MockNode::make(
        MockNode::ColumnDefinitions{{DataType::Int, "a"}, {DataType::Int, "b"}, {DataType::Int, "c"}}, "e");
    e_a = node_e->get_column("a");
    e_b = node_e->get_column("b");
    e_c = node_e->get_column("c");

    lineitem = MockNode::make(MockNode::ColumnDefinitions{{DataType::Int, "extendedprice"},
                                                          {DataType::Int, "quantity"},
                                                          {DataType::Int, "partkey"},
                                                          {DataType::String, "shipdate"},
                                                          {DataType::Int, "suppkey"}},
                              "lineitem");
    l_extendedprice = lineitem->get_column("extendedprice");
    l_quantity = lineitem->get_column("quantity");
    l_partkey = lineitem->get_column("partkey");
    l_shipdate = lineitem->get_column("shipdate");
    l_suppkey = lineitem->get_column("suppkey");

    nation =
        MockNode::make(MockNode::ColumnDefinitions{{DataType::Int, "nationkey"}, {DataType::Int, "name"}}, "nation");
    n_nationkey = nation->get_column("nationkey");
    n_name = nation->get_column("name");

    part = MockNode::make(MockNode::ColumnDefinitions{{DataType::Int, "partkey"},
                                                      {DataType::String, "brand"},
                                                      {DataType::String, "container"},
                                                      {DataType::String, "name"}});
    p_partkey = part->get_column("partkey");
    p_brand = part->get_column("brand");
    p_container = part->get_column("container");
    p_name = part->get_column("name");

    partsupp = MockNode::make(
        MockNode::ColumnDefinitions{
            {DataType::Int, "availqty"}, {DataType::Int, "partkey"}, {DataType::Int, "suppkey"}},
        "partsupp");
    ps_availqty = partsupp->get_column("availqty");
    ps_partkey = partsupp->get_column("partkey");
    ps_suppkey = partsupp->get_column("suppkey");

    supplier = MockNode::make(MockNode::ColumnDefinitions{{DataType::Int, "suppkey"},
                                                          {DataType::String, "address"},
                                                          {DataType::String, "name"},
                                                          {DataType::Int, "nationkey"}},
                              "supplier");
    s_suppkey = supplier->get_column("suppkey");
    s_address = supplier->get_column("address");
    s_name = supplier->get_column("name");
    s_nationkey = supplier->get_column("nationkey");

    _rule = std::make_shared<SubqueryToJoinRule>();
  }

  std::shared_ptr<SubqueryToJoinRule> _rule;

  std::shared_ptr<MockNode> node_a, node_b, node_c, node_d, node_e, lineitem, nation, part, partsupp, supplier;
  LQPColumnReference a_a, a_b, a_c, b_a, b_b, c_a, d_a, d_b, d_c, e_a, e_b, e_c, l_extendedprice, l_quantity, l_partkey,
      l_shipdate, l_suppkey, n_nationkey, n_name, p_partkey, p_brand, p_container, p_name, ps_availqty, ps_partkey,
      ps_suppkey, s_suppkey, s_address, s_name, s_nationkey;
  std::shared_ptr<LQPColumnExpression> a_a_expression, a_b_expression, a_c_expression;
};
