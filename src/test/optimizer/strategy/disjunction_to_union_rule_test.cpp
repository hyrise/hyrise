#include "gtest/gtest.h"

#include "logical_query_plan/union_node.hpp"
#include "optimizer/strategy/disjunction_to_union_rule.hpp"
#include "strategy_base_test.hpp"
#include "testing_assert.hpp"

namespace opossum {

class DisjunctionToUnionRuleTest : public StrategyBaseTest {
 public:
  void SetUp() override {
    node_a = MockNode::make(MockNode::ColumnDefinitions{{DataType::Int, "a"}, {DataType::Int, "b"}}, "a");
    a_a = node_a->get_column("a");
    a_b = node_a->get_column("b");

    node_b = MockNode::make(MockNode::ColumnDefinitions{{DataType::Int, "a"}, {DataType::Int, "b"}}, "b");
    b_a = node_b->get_column("a");
    b_b = node_b->get_column("b");

    node_c = MockNode::make(MockNode::ColumnDefinitions{{DataType::Int, "a"}, {DataType::Int, "b"}}, "c");
    c_a = node_c->get_column("a");
    c_b = node_c->get_column("b");

    node_d = MockNode::make(MockNode::ColumnDefinitions{{DataType::Int, "a"}, {DataType::Int, "b"}}, "d");
    d_a = node_d->get_column("a");
    d_b = node_d->get_column("b");

    node_e = MockNode::make(MockNode::ColumnDefinitions{{DataType::Int, "a"}, {DataType::Int, "b"}}, "e");
    e_a = node_e->get_column("a");
    e_b = node_e->get_column("b");

    customer_demographics =
        MockNode::make(MockNode::ColumnDefinitions{{DataType::Int, "c_customer_sk"}}, "customer_demographics");
    c_customer_sk = customer_demographics->get_column("c_customer_sk");

    date_dim = MockNode::make(
        MockNode::ColumnDefinitions{{DataType::Int, "d_date_sk"}, {DataType::Int, "d_year"}, {DataType::Int, "d_qoy"}},
        "date_dim");
    d_date_sk = date_dim->get_column("d_date_sk");
    d_year = date_dim->get_column("d_year");
    d_qoy = date_dim->get_column("d_qoy");

    web_sales = MockNode::make(
        MockNode::ColumnDefinitions{{DataType::Int, "ws_sold_date_sk"}, {DataType::Int, "ws_bill_customer_sk"}},
        "web_sales");
    ws_sold_date_sk = web_sales->get_column("ws_sold_date_sk");
    ws_bill_customer_sk = web_sales->get_column("ws_bill_customer_sk");

    catalog_sales = MockNode::make(
        MockNode::ColumnDefinitions{{DataType::Int, "cs_sold_date_sk"}, {DataType::Int, "cs_ship_customer_sk"}},
        "catalog_sales");
    cs_sold_date_sk = catalog_sales->get_column("cs_sold_date_sk");
    cs_ship_customer_sk = catalog_sales->get_column("cs_ship_customer_sk");

    _rule = std::make_shared<DisjunctionToUnionRule>();
  }

  std::shared_ptr<DisjunctionToUnionRule> _rule;

  std::shared_ptr<MockNode> node_a, node_b, node_c, node_d, node_e, customer_demographics, date_dim, web_sales,
      catalog_sales;
  LQPColumnReference a_a, a_b, b_a, b_b, c_a, c_b, d_a, d_b, e_a, e_b, c_customer_sk, d_date_sk, d_year, d_qoy,
      ws_sold_date_sk, ws_bill_customer_sk, cs_sold_date_sk, cs_ship_customer_sk;
};

TEST_F(DisjunctionToUnionRuleTest, TwoExistsToUnion) {
  // SELECT * FROM a WHERE EXISTS (SELECT * FROM b WHERE b.b = a.b) OR EXISTS (SELECT * FROM c WHERE c.b = a.b)

  const auto parameter = correlated_parameter_(ParameterID{0}, a_b);

  // clang-format off
  const auto subquery_lqp_a =
  PredicateNode::make(equals_(b_b, parameter),
    node_b);

  const auto subquery_a = lqp_subquery_(subquery_lqp_a, std::make_pair(ParameterID{0}, a_b));

  const auto subquery_lqp_b =
  PredicateNode::make(equals_(c_b, parameter),
    node_c);

  const auto subquery_b = lqp_subquery_(subquery_lqp_b, std::make_pair(ParameterID{0}, a_b));

  const auto input_lqp =
  PredicateNode::make(or_(exists_(subquery_a), exists_(subquery_b)),
    node_a);

  const auto expected_lqp =
  UnionNode::make(UnionMode::Positions,
    PredicateNode::make(exists_(subquery_a),
      node_a),
    PredicateNode::make(exists_(subquery_b),
      node_a));
  // clang-format on

  const auto actual_lqp = StrategyBaseTest::apply_rule(_rule, input_lqp);

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(DisjunctionToUnionRuleTest, FourExistsToUnion) {
  // SELECT * FROM a WHERE EXISTS (
  //   SELECT * FROM b WHERE b.b = a.b
  // ) OR EXISTS (
  //   SELECT * FROM c WHERE c.b = a.b
  // ) OR EXISTS (
  //   SELECT * FROM d WHERE d.b = a.b
  // ) OR EXISTS (
  //   SELECT * FROM e WHERE e.b = e.b
  // )

  const auto parameter = correlated_parameter_(ParameterID{0}, a_b);

  // clang-format off
  const auto subquery_lqp_a =
  PredicateNode::make(equals_(b_b, parameter),
    node_b);

  const auto subquery_a = lqp_subquery_(subquery_lqp_a, std::make_pair(ParameterID{0}, a_b));

  const auto subquery_lqp_b =
  PredicateNode::make(equals_(c_b, parameter),
    node_c);

  const auto subquery_b = lqp_subquery_(subquery_lqp_b, std::make_pair(ParameterID{0}, a_b));

  const auto subquery_lqp_c =
  PredicateNode::make(equals_(d_b, parameter),
    node_d);

  const auto subquery_c = lqp_subquery_(subquery_lqp_c, std::make_pair(ParameterID{0}, a_b));

  const auto subquery_lqp_d =
  PredicateNode::make(equals_(e_b, parameter),
                      node_e);

  const auto subquery_d = lqp_subquery_(subquery_lqp_d, std::make_pair(ParameterID{0}, a_b));

  const auto input_lqp =
  PredicateNode::make(or_(exists_(subquery_a), or_(exists_(subquery_b), or_(exists_(subquery_c), exists_(subquery_d)))),
    node_a);

  const auto expected_lqp =
  UnionNode::make(UnionMode::Positions,
    PredicateNode::make(exists_(subquery_a),
      node_a),
    UnionNode::make(UnionMode::Positions,
      PredicateNode::make(exists_(subquery_b),
        node_a),
      UnionNode::make(UnionMode::Positions,
        PredicateNode::make(exists_(subquery_c),
          node_a),
        PredicateNode::make(exists_(subquery_d),
          node_a))));

  const auto actual_lqp = StrategyBaseTest::apply_rule(_rule, input_lqp);

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(DisjunctionToUnionRuleTest, SelectColumn) {
  // SELECT * FROM a WHERE 1 OR 3 > 2

  // clang-format off
  const auto input_lqp =
  ProjectionNode::make(expression_vector(a_a),
    PredicateNode::make(or_(value_(1), greater_than_(value_(3), value_(2))),
      node_a));

  const auto expected_lqp =
  ProjectionNode::make(expression_vector(a_a),
    UnionNode::make(UnionMode::Positions,
      PredicateNode::make(value_(1),
        node_a),
      PredicateNode::make(greater_than_(value_(3), value_(2)),
        node_a)));
  // clang-format on

  const auto actual_lqp = StrategyBaseTest::apply_rule(_rule, input_lqp);

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(DisjunctionToUnionRuleTest, OptimizeTPCDS35) {
  // Optimize a simplified and pre-optimized part of TPC-H 35, as the DisjunctionToUnionRule would receive it
  //  select *
  //  from customer_demographics
  //  (exists (select *
  //          from web_sales, date_dim
  //          where c_customer_sk = ws_bill_customer_sk and
  //          ws_sold_date_sk = d_date_sk and
  //          d_year = 2019 and
  //          d_qoy < 4) or
  //  exists (select *
  //          from catalog_sales, date_dim
  //          where c_customer_sk = cs_ship_customer_sk and
  //          cs_sold_date_sk = d_date_sk and
  //          d_year = 2019 and
  //          d_qoy < 4))

  const auto parameter = correlated_parameter_(ParameterID{0}, c_customer_sk);

  // clang-format off
  const auto subquery_lqp_a =
  JoinNode::make(JoinMode::Inner, equals_(ws_sold_date_sk, d_date_sk),
    PredicateNode::make(equals_(parameter, ws_bill_customer_sk),
      web_sales),
    PredicateNode::make(less_than_(d_qoy, value_(4)),
      PredicateNode::make(equals_(d_year, value_(2019)),
        date_dim)));

  const auto subquery_a = lqp_subquery_(subquery_lqp_a, std::make_pair(ParameterID{0}, c_customer_sk));

  const auto subquery_lqp_b =
  JoinNode::make(JoinMode::Inner, equals_(cs_sold_date_sk, d_date_sk),
    PredicateNode::make(equals_(parameter, cs_ship_customer_sk),
      catalog_sales),
    PredicateNode::make(less_than_(d_qoy, value_(4)),
      PredicateNode::make(equals_(d_year, value_(2019)),
        date_dim)));

  const auto subquery_b = lqp_subquery_(subquery_lqp_b, std::make_pair(ParameterID{0}, c_customer_sk));

  const auto input_lqp =
  PredicateNode::make(or_(exists_(subquery_a), exists_(subquery_b)),
    customer_demographics);

  const auto expected_lqp =
  UnionNode::make(UnionMode::Positions,
    PredicateNode::make(exists_(subquery_a),
      customer_demographics),
    PredicateNode::make(exists_(subquery_b),
      customer_demographics));
  // clang-format on

  const auto actual_lqp = StrategyBaseTest::apply_rule(_rule, input_lqp);

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

}  // namespace opossum
