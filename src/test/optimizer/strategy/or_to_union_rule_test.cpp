#include "gtest/gtest.h"

#include "strategy_base_test.hpp"
#include "testing_assert.hpp"
#include "logical_query_plan/union_node.hpp"
#include "optimizer/strategy/or_to_union_rule.hpp"

namespace opossum {

  class OrToUnionRuleTest : public StrategyBaseTest {
  public:
    void SetUp() override {
      node_a = MockNode::make(
      MockNode::ColumnDefinitions{{DataType::Int, "a"}, {DataType::Int, "b"}, {DataType::Int, "c"}}, "a");
      a_a = node_a->get_column("a");
      a_b = node_a->get_column("b");
      a_c = node_a->get_column("c");

      node_b = MockNode::make(MockNode::ColumnDefinitions{{DataType::Int, "a"}, {DataType::Int, "b"}}, "b");
      b_a = node_b->get_column("a");
      b_b = node_b->get_column("b");

      node_c = MockNode::make(
      MockNode::ColumnDefinitions{{DataType::Int, "a"}, {DataType::Int, "b"}, {DataType::Int, "c"}}, "c");
      c_a = node_c->get_column("a");
      c_b = node_c->get_column("b");

      customer_demographics = MockNode::make(MockNode::ColumnDefinitions{{DataType::Int, "c_customer_sk"}},
                                             "customer_demographics");
      c_customer_sk = customer_demographics->get_column("c_customer_sk");

      date_dim = MockNode::make(MockNode::ColumnDefinitions{{DataType::Int, "d_date_sk"},
                                                            {DataType::Int, "d_year"},
                                                            {DataType::Int, "d_qoy"}},
                                                            "date_dim");
      d_date_sk = date_dim->get_column("d_date_sk");
      d_year = date_dim->get_column("d_year");
      d_qoy = date_dim->get_column("d_qoy");

//      store_sales = MockNode::make(MockNode::ColumnDefinitions{{DataType::Int, "ss_sold_date_sk"}, "store_sales");
//      ss_sold_date_sk = store_sales->get_column("ss_sold_date_sk");

      web_sales = MockNode::make(MockNode::ColumnDefinitions{{DataType::Int, "ws_sold_date_sk"},
                                                            {DataType::Int, "ws_bill_customer_sk"}},
                                                            "web_sales");
      ws_sold_date_sk = web_sales->get_column("ws_sold_date_sk");
      ws_bill_customer_sk = web_sales->get_column("ws_bill_customer_sk");

      catalog_sales = MockNode::make(MockNode::ColumnDefinitions{{DataType::Int, "cs_sold_date_sk"},
                                                                 {DataType::Int, "cs_ship_customer_sk"}},
                                                                 "catalog_sales");
      cs_sold_date_sk = catalog_sales->get_column("cs_sold_date_sk");
      cs_ship_customer_sk = catalog_sales->get_column("cs_ship_customer_sk");

      _rule = std::make_shared<OrToUnionRule>();
    }

    std::shared_ptr<OrToUnionRule> _rule;

    std::shared_ptr<MockNode> node_a, node_b, node_c, customer_demographics, date_dim, store_sales, web_sales, catalog_sales;
    LQPColumnReference a_a, a_b, a_c, b_a, b_b, c_a, c_b, c_customer_sk, d_date_sk, d_year, d_qoy,
      ss_sold_date_sk, ws_sold_date_sk, ws_bill_customer_sk, cs_sold_date_sk, cs_ship_customer_sk;
  };

TEST_F(OrToUnionRuleTest, TwoExistsToUnion) {
  // select * from nation where exists (select * from customer where c_nationkey = n_nationkey) or exists (select * from supplier where s_nationkey = n_nationkey);
  // SELECT * FROM a WHERE EXISTS (SELECT * FROM b WHERE b.b = a.b) OR EXISTS (SELECT * FROM c WHERE c.b = a.b)

  const auto parameter = correlated_parameter_(ParameterID{0}, a_a);

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

TEST_F(OrToUnionRuleTest, NoRewriteThreeExistsToUnion) {
  // Not yet implemented because it is not needed in TPC-DS
  // SELECT * FROM a WHERE EXISTS (
  //   SELECT * FROM b WHERE b.b = a.b
  // ) OR EXISTS (
  //   SELECT * FROM c WHERE c.b = a.b
  // ) OR EXISTS (
  //   SELECT * FROM d WHERE d.b = a.b
  // )

  const auto parameter = correlated_parameter_(ParameterID{0}, a_a);

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
  PredicateNode::make(equals_(c_b, parameter),
    node_c);

  const auto subquery_c = lqp_subquery_(subquery_lqp_c, std::make_pair(ParameterID{0}, a_b));

  const auto input_lqp =
  PredicateNode::make(or_(exists_(subquery_a), or_(exists_(subquery_b), exists_(subquery_c))),
    node_a);

  const auto expected_lqp = input_lqp->deep_copy();
  // clang-format on

  const auto actual_lqp = StrategyBaseTest::apply_rule(_rule, input_lqp);

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(OrToUnionRuleTest, SelectColumn) {
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

  std::cout << "INPUT\n" << *input_lqp << "\n\n";
  std::cout << "ACTUAL\n" << *actual_lqp << "\n\n";
  std::cout << "EXPECTED\n" << *expected_lqp << "\n\n";

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(OrToUnionRuleTest, OptimizeTPCDS35) {
  // Optimize a simplified and pre-optimized part of TPC-H 35, as the OrToUnionRule would receive it
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
