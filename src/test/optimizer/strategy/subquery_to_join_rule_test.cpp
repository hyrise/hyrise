#include "strategy_base_test.hpp"

#include "expression/expression_functional.hpp"
#include "expression/expression_utils.hpp"
#include "expression/lqp_column_expression.hpp"
#include "logical_query_plan/abstract_lqp_node.hpp"
#include "logical_query_plan/aggregate_node.hpp"
#include "logical_query_plan/join_node.hpp"
#include "logical_query_plan/limit_node.hpp"
#include "logical_query_plan/mock_node.hpp"
#include "logical_query_plan/predicate_node.hpp"
#include "logical_query_plan/projection_node.hpp"
#include "logical_query_plan/sort_node.hpp"
#include "logical_query_plan/stored_table_node.hpp"
#include "logical_query_plan/validate_node.hpp"
#include "optimizer/strategy/join_predicate_ordering_rule.hpp"
#include "optimizer/strategy/subquery_to_join_rule.hpp"
#include "utils/load_table.hpp"

using namespace opossum::expression_functional;  // NOLINT

namespace opossum {

class SubqueryToJoinRuleTest : public StrategyBaseTest {
 public:
  void SetUp() override {
    // One size fits all - we only look at cardinalities, anyway.
    const auto histogram = GenericHistogram<int32_t>::with_single_bin(1, 100, 100, 10);
    const auto string_histogram = GenericHistogram<pmr_string>::with_single_bin("a", "z", 100, 10);

    node_a = create_mock_node_with_statistics({{DataType::Int, "a"}, {DataType::Int, "b"}, {DataType::Int, "c"}}, 100,
                                              {histogram, histogram, histogram});
    a_a = node_a->get_column("a");
    a_b = node_a->get_column("b");
    a_c = node_a->get_column("c");
    a_a_expression = to_expression(a_a);
    a_b_expression = to_expression(a_b);
    a_c_expression = to_expression(a_c);

    node_b =
        create_mock_node_with_statistics({{DataType::Int, "a"}, {DataType::Int, "b"}}, 100, {histogram, histogram});
    b_a = node_b->get_column("a");
    b_b = node_b->get_column("b");

    node_c = create_mock_node_with_statistics({{DataType::Int, "a"}, {DataType::Int, "b"}, {DataType::Int, "c"}}, 100,
                                              {histogram, histogram, histogram});
    c_a = node_c->get_column("a");

    node_d = create_mock_node_with_statistics({{DataType::Int, "a"}, {DataType::Int, "b"}, {DataType::Int, "c"}}, 100,
                                              {histogram, histogram, histogram});
    d_a = node_d->get_column("a");
    d_b = node_d->get_column("b");
    d_c = node_d->get_column("c");

    node_e = create_mock_node_with_statistics({{DataType::Int, "a"}, {DataType::Int, "b"}, {DataType::Int, "c"}}, 100,
                                              {histogram, histogram, histogram});
    e_a = node_e->get_column("a");
    e_b = node_e->get_column("b");
    e_c = node_e->get_column("c");

    small_node =
        create_mock_node_with_statistics({{DataType::Int, "a"}, {DataType::Int, "b"}}, 100, {histogram, histogram});
    small_node_a = small_node->get_column("a");

    large_node =
        create_mock_node_with_statistics({{DataType::Int, "a"}, {DataType::Int, "b"}}, 10000, {histogram, histogram});
    large_node_a = large_node->get_column("a");

    lineitem = create_mock_node_with_statistics({{DataType::Int, "extendedprice"},
                                                 {DataType::Int, "quantity"},
                                                 {DataType::Int, "partkey"},
                                                 {DataType::String, "shipdate"},
                                                 {DataType::Int, "suppkey"}},
                                                100, {histogram, histogram, histogram, string_histogram, histogram});
    l_extendedprice = lineitem->get_column("extendedprice");
    l_quantity = lineitem->get_column("quantity");
    l_partkey = lineitem->get_column("partkey");
    l_shipdate = lineitem->get_column("shipdate");
    l_suppkey = lineitem->get_column("suppkey");

    nation = create_mock_node_with_statistics({{DataType::Int, "nationkey"}, {DataType::Int, "name"}}, 100,
                                              {histogram, histogram});
    n_nationkey = nation->get_column("nationkey");
    n_name = nation->get_column("name");

    part = create_mock_node_with_statistics({{DataType::Int, "partkey"},
                                             {DataType::String, "brand"},
                                             {DataType::String, "container"},
                                             {DataType::String, "name"}},
                                            100, {histogram, string_histogram, string_histogram, string_histogram});
    p_partkey = part->get_column("partkey");
    p_brand = part->get_column("brand");
    p_container = part->get_column("container");
    p_name = part->get_column("name");

    partsupp = create_mock_node_with_statistics(
        {{DataType::Int, "availqty"}, {DataType::Int, "partkey"}, {DataType::Int, "suppkey"}}, 100,
        {histogram, histogram, histogram});
    ps_availqty = partsupp->get_column("availqty");
    ps_partkey = partsupp->get_column("partkey");
    ps_suppkey = partsupp->get_column("suppkey");

    supplier = create_mock_node_with_statistics({{DataType::Int, "suppkey"},
                                                 {DataType::String, "address"},
                                                 {DataType::String, "name"},
                                                 {DataType::Int, "nationkey"}},
                                                100, {histogram, string_histogram, string_histogram, histogram});
    s_suppkey = supplier->get_column("suppkey");
    s_address = supplier->get_column("address");
    s_name = supplier->get_column("name");
    s_nationkey = supplier->get_column("nationkey");

    _rule = std::make_shared<SubqueryToJoinRule>();
  }

  std::shared_ptr<SubqueryToJoinRule> _rule;

  std::shared_ptr<MockNode> node_a, node_b, node_c, node_d, node_e, small_node, large_node, lineitem, nation, part,
      partsupp, supplier;
  LQPColumnReference a_a, a_b, a_c, b_a, b_b, c_a, d_a, d_b, d_c, e_a, e_b, e_c, small_node_a, large_node_a,
      l_extendedprice, l_quantity, l_partkey, l_shipdate, l_suppkey, n_nationkey, n_name, p_partkey, p_brand,
      p_container, p_name, ps_availqty, ps_partkey, ps_suppkey, s_suppkey, s_address, s_name, s_nationkey;
  std::shared_ptr<LQPColumnExpression> a_a_expression, a_b_expression, a_c_expression;
};

// HELPER FUNCTION TESTS

TEST_F(SubqueryToJoinRuleTest, AssessCorrelatedParameterUsageCountsNodesNotUsages) {
  const auto parameter0 = correlated_parameter_(ParameterID{0}, a_a);
  const auto parameter1 = correlated_parameter_(ParameterID{1}, a_b);
  const std::map<ParameterID, std::shared_ptr<AbstractExpression>> parameter_map = {{ParameterID{0}, a_a_expression},
                                                                                    {ParameterID{1}, a_b_expression}};

  // clang-format off
  const auto lqp =
  PredicateNode::make(equals_(b_a, parameter0),
    PredicateNode::make(and_(equals_(b_b, parameter0), equals_(b_b, parameter1)),
      node_b));
  // clang-format on

  const auto result = SubqueryToJoinRule::assess_correlated_parameter_usage(lqp, parameter_map);
  EXPECT_EQ(result, std::pair(true, size_t{2}));
}

TEST_F(SubqueryToJoinRuleTest, AssessCorrelatedParameterUsageIgnoresUnrelatedParameters) {
  const auto unrelated_parameter = correlated_parameter_(ParameterID{0}, a_a);
  const std::map<ParameterID, std::shared_ptr<AbstractExpression>> parameter_map = {};

  // Would return not optimizable for relevant parameter
  // clang-format off
  const auto lqp =
  ProjectionNode::make(expression_vector(add_(b_a, unrelated_parameter)),
    node_a);
  // clang-format on

  const auto result = SubqueryToJoinRule::assess_correlated_parameter_usage(lqp, parameter_map);
  EXPECT_EQ(result, std::pair(true, size_t{0}));
}

TEST_F(SubqueryToJoinRuleTest, AssessCorrelatedParameterUsageFindsUsagesInSubqueries) {
  const auto parameter = correlated_parameter_(ParameterID{0}, a_a);
  const std::map<ParameterID, std::shared_ptr<AbstractExpression>> parameter_map = {{ParameterID{0}, a_a_expression}};

  // clang-format off
  const auto subquery_lqp =
  PredicateNode::make(equals_(parameter, b_a),
    node_b);

  const auto lqp =
  PredicateNode::make(exists_(lqp_subquery_(subquery_lqp)),
    node_a);
  // clang-format on

  const auto& [optimizable, _] = SubqueryToJoinRule::assess_correlated_parameter_usage(lqp, parameter_map);
  EXPECT_FALSE(optimizable);
}

TEST_F(SubqueryToJoinRuleTest, AssessCorrelatedParameterUsageReportsUnoptimizableUsageInProjection) {
  const auto parameter = correlated_parameter_(ParameterID{0}, a_a);
  const std::map<ParameterID, std::shared_ptr<AbstractExpression>> parameter_map = {{ParameterID{0}, a_a_expression}};

  // clang-format off
  const auto lqp =
  ProjectionNode::make(expression_vector(add_(b_a, parameter)),
    node_b);
  // clang-format on

  const auto& [optimizable, _] = SubqueryToJoinRule::assess_correlated_parameter_usage(lqp, parameter_map);
  EXPECT_FALSE(optimizable);
}

TEST_F(SubqueryToJoinRuleTest, AssessCorrelatedParameterUsageReportsUnoptimizableUsageInJoin) {
  const auto parameter = correlated_parameter_(ParameterID{0}, a_a);
  const std::map<ParameterID, std::shared_ptr<AbstractExpression>> parameter_map = {{ParameterID{0}, a_a_expression}};

  // clang-format off
  const auto lqp =
  JoinNode::make(JoinMode::Inner, expression_vector(equals_(b_a, c_a), equals_(b_a, parameter)),
    node_b,
    node_c);
  // clang-format on

  const auto& [optimizable, _] = SubqueryToJoinRule::assess_correlated_parameter_usage(lqp, parameter_map);
  EXPECT_FALSE(optimizable);
}

TEST_F(SubqueryToJoinRuleTest, AdaptAggregateNode) {
  const auto aggregate_node = AggregateNode::make(expression_vector(a_a), expression_vector());
  const auto& original_expressions = aggregate_node->column_expressions();

  // a_a is already group by expression, check it is not added again
  const auto adapted_aggregate_node = SubqueryToJoinRule::adapt_aggregate_node(aggregate_node, {a_a_expression});
  EXPECT_EQ(adapted_aggregate_node->column_expressions().size(), size_t{1});

  // a_b is an additional required group by expression, check it is added
  EXPECT_EQ(std::find(original_expressions.cbegin(), original_expressions.cend(), a_b_expression),
            original_expressions.cend());
  const auto adapted_aggregate_node_2 = SubqueryToJoinRule::adapt_aggregate_node(aggregate_node, {a_b_expression});
  const auto& expressions = adapted_aggregate_node_2->column_expressions();
  EXPECT_NE(std::find(expressions.cbegin(), expressions.cend(), a_b_expression), expressions.cend());
}

TEST_F(SubqueryToJoinRuleTest, AdaptAliasNode) {
  const auto alias_node =
      AliasNode::make(expression_vector(a_a, a_a, a_b), std::vector<std::string>{"a_a", "alias_a_a", "alias_a_b"});
  const auto& original_expressions = alias_node->column_expressions();

  // no added duplicates, preserve multiple names for same column,
  const auto adapted_alias_node = SubqueryToJoinRule::adapt_alias_node(alias_node, {a_a_expression});
  EXPECT_EQ(adapted_alias_node->column_expressions().size(), size_t{3});

  // no additional aliases
  const auto adapted_alias_node2 = SubqueryToJoinRule::adapt_alias_node(alias_node, {a_b_expression});
  EXPECT_EQ(adapted_alias_node2->column_expressions().size(), size_t{3});

  // add if necessary
  EXPECT_EQ(std::find(original_expressions.cbegin(), original_expressions.cend(), a_c_expression),
            original_expressions.cend());
  const auto adapted_alias_node3 = SubqueryToJoinRule::adapt_alias_node(alias_node, {a_c_expression});
  const auto& expressions = adapted_alias_node3->column_expressions();
  EXPECT_NE(std::find(expressions.cbegin(), expressions.cend(), a_c_expression), expressions.cend());
}

TEST_F(SubqueryToJoinRuleTest, AdaptProjectionNode) {
  const auto projection_node = ProjectionNode::make(expression_vector(a_a, a_a));
  const auto& original_expressions = projection_node->column_expressions();

  // no added duplicates, preserve original duplicates
  const auto adapted_projection_node = SubqueryToJoinRule::adapt_projection_node(projection_node, {a_a_expression});
  EXPECT_EQ(adapted_projection_node->column_expressions().size(), size_t{2});

  // add if necessary
  EXPECT_EQ(std::find(original_expressions.cbegin(), original_expressions.cend(), a_b_expression),
            original_expressions.cend());
  const auto adapted_projection_node2 = SubqueryToJoinRule::adapt_projection_node(projection_node, {a_b_expression});
  const auto& expressions = adapted_projection_node2->column_expressions();
  EXPECT_NE(std::find(expressions.cbegin(), expressions.cend(), a_b_expression), expressions.cend());
}

TEST_F(SubqueryToJoinRuleTest, TryToExtractJoinPredicatesSuccessCase) {
  const auto parameter = correlated_parameter_(ParameterID{0}, a_a);
  const std::map<ParameterID, std::shared_ptr<AbstractExpression>> parameter_map = {{ParameterID{0}, a_a_expression}};
  const auto predicate_node = PredicateNode::make(equals_(b_a, parameter), node_b);

  const auto& [extracted_predicates, remaining_expression] =
      SubqueryToJoinRule::try_to_extract_join_predicates(predicate_node, parameter_map, false);
  EXPECT_FALSE(remaining_expression);
  ASSERT_EQ(extracted_predicates.size(), 1);
  EXPECT_EQ(*extracted_predicates.front(), *equals_(a_a, b_a));
}

TEST_F(SubqueryToJoinRuleTest, TryToExtractJoinPredicatesUnsupportedPredicateTypes) {
  const auto parameter = correlated_parameter_(ParameterID{0}, a_a);
  const std::map<ParameterID, std::shared_ptr<AbstractExpression>> parameter_map = {{ParameterID{0}, a_a_expression}};

  auto unsupported_predicates = std::vector<std::shared_ptr<AbstractExpression>>{
      exists_(lqp_subquery_(node_a)), in_(parameter, list_(1)), between_inclusive_(parameter, b_b, value_(100)),
      like_(parameter, "%test%"),     is_null_(parameter),
  };

  for (const auto& predicate : unsupported_predicates) {
    const auto lqp = PredicateNode::make(predicate, node_b);
    const auto& [extracted_predicates, remaining_expression] =
        SubqueryToJoinRule::try_to_extract_join_predicates(lqp, parameter_map, false);
    EXPECT_TRUE(extracted_predicates.empty());
    ASSERT_TRUE(remaining_expression);
    EXPECT_EQ(*remaining_expression, *predicate);
  }
}

TEST_F(SubqueryToJoinRuleTest, TryToExtractJoinPredicatesNonEqualsPredicateBelowAggregate) {
  const auto parameter = correlated_parameter_(ParameterID{0}, a_a);
  const auto predicate_node = PredicateNode::make(less_than_(b_b, parameter), node_b);
  const auto& [extracted_predicates, remaining_expression] =
      SubqueryToJoinRule::try_to_extract_join_predicates(predicate_node, {{ParameterID{0}, a_a_expression}}, true);
  EXPECT_TRUE(extracted_predicates.empty());
  ASSERT_TRUE(remaining_expression);
  EXPECT_EQ(*remaining_expression, *predicate_node->predicate());
}

TEST_F(SubqueryToJoinRuleTest, TryToExtractJoinPredicatesNonParameterSideMustBeAColumnExpression) {
  const auto parameter = correlated_parameter_(ParameterID{0}, a_b);
  const std::map<ParameterID, std::shared_ptr<AbstractExpression>> parameter_map = {{ParameterID{0}, a_a_expression}};

  const auto rejected_predicate_node = PredicateNode::make(equals_(add_(b_a, 2), parameter), node_b);
  const auto& [rejected_extracted, rejected_remaining] =
      SubqueryToJoinRule::try_to_extract_join_predicates(rejected_predicate_node, parameter_map, false);
  EXPECT_TRUE(rejected_extracted.empty());
  ASSERT_TRUE(rejected_remaining);
  EXPECT_EQ(*rejected_remaining, *rejected_predicate_node->predicate());

  // clang-format off
  const auto accepted_predicate_node =
  PredicateNode::make(equals_(add_(b_a, 2), parameter),
    ProjectionNode::make(expression_vector(add_(b_a, 2)),
      node_b));
  // clang-format on

  const auto& [accepted_extracted, accepted_remaining] =
      SubqueryToJoinRule::try_to_extract_join_predicates(accepted_predicate_node, parameter_map, false);
  EXPECT_FALSE(accepted_extracted.empty());
  EXPECT_FALSE(accepted_remaining);
}

TEST_F(SubqueryToJoinRuleTest, TryToExtractJoinPredicatesUnrelatedParameter) {
  const auto relevant_parameter = correlated_parameter_(ParameterID{0}, a_a);
  const auto unrelated_parameter = correlated_parameter_(ParameterID{1}, c_a);
  const std::map<ParameterID, std::shared_ptr<AbstractExpression>> parameter_map = {{ParameterID{0}, a_a_expression}};

  const auto predicate_node = PredicateNode::make(equals_(unrelated_parameter, b_a), node_b);
  const auto& [extracted_predicates, remaining_expression] =
      SubqueryToJoinRule::try_to_extract_join_predicates(predicate_node, parameter_map, false);
  EXPECT_TRUE(extracted_predicates.empty());
  ASSERT_TRUE(remaining_expression);
  EXPECT_EQ(*remaining_expression, *predicate_node->predicate());
}

TEST_F(SubqueryToJoinRuleTest, TryToExtractJoinPredicatesHandlesAndedExpressions) {
  const auto parameter = correlated_parameter_(ParameterID{0}, a_b);
  const std::map<ParameterID, std::shared_ptr<AbstractExpression>> parameter_map = {{ParameterID{0}, a_a_expression}};

  const auto expression = and_(equals_(parameter, b_a), and_(equals_(b_a, b_b), less_than_(b_b, parameter)));
  const auto predicate_node = PredicateNode::make(expression, node_b);

  const auto& [extracted_predicates, remaining_expression] =
      SubqueryToJoinRule::try_to_extract_join_predicates(predicate_node, parameter_map, false);
  EXPECT_EQ(extracted_predicates.size(), 2);
  ASSERT_TRUE(remaining_expression);
  EXPECT_EQ(*remaining_expression, *equals_(b_a, b_b));
}

TEST_F(SubqueryToJoinRuleTest, PullUpCorrelatedPredicatesCanPullEqualsFromBelowAggregate) {
  const auto parameter = correlated_parameter_(ParameterID{0}, a_a);
  const std::map<ParameterID, std::shared_ptr<AbstractExpression>> parameter_map = {{ParameterID{0}, a_a_expression}};

  // clang-format off
  const auto lqp =
  AggregateNode::make(expression_vector(), expression_vector(max_(b_a)),
    PredicateNode::make(equals_(b_a, parameter),
      node_b));
  // clang-format on

  const auto result = SubqueryToJoinRule::pull_up_correlated_predicates(lqp, parameter_map);
  ASSERT_EQ(result.join_predicates.size(), 1);
  EXPECT_EQ(*result.join_predicates.front(), *equals_(a_a, b_a));
  // Changes to the LQP are tested in AdaptAggregateNode
}

TEST_F(SubqueryToJoinRuleTest, PullUpCorrelatedPredicatesCannotPullNonEqualsFromBelowAggregate) {
  const auto parameter = correlated_parameter_(ParameterID{0}, a_a);
  const std::map<ParameterID, std::shared_ptr<AbstractExpression>> parameter_map = {{ParameterID{0}, a_a_expression}};

  // clang-format off
  const auto lqp =
  AggregateNode::make(expression_vector(), expression_vector(max_(b_a)),
    PredicateNode::make(less_than_(b_a, parameter),
      node_b));
  // clang-format on

  const auto result = SubqueryToJoinRule::pull_up_correlated_predicates(lqp, parameter_map);
  EXPECT_TRUE(result.join_predicates.empty());
  EXPECT_LQP_EQ(result.adapted_lqp, lqp);
}

TEST_F(SubqueryToJoinRuleTest, PullUpCorrelatedPredicatesCanPullFromBelowAlias) {
  const auto parameter = correlated_parameter_(ParameterID{0}, a_a);
  const std::map<ParameterID, std::shared_ptr<AbstractExpression>> parameter_map = {{ParameterID{0}, a_a_expression}};

  // clang-format off
  const auto lqp =
  AliasNode::make(expression_vector(b_b), std::vector<std::string>{"alias"},
    PredicateNode::make(less_than_(b_a, parameter),
      node_b));
  // clang-format on

  const auto result = SubqueryToJoinRule::pull_up_correlated_predicates(lqp, parameter_map);
  ASSERT_EQ(result.join_predicates.size(), 1);
  EXPECT_EQ(*result.join_predicates.front(), *greater_than_(a_a, b_a));
  // Changes to the LQP are tested in AdaptAliasNode
}

TEST_F(SubqueryToJoinRuleTest, PullUpCorrelatedPredicatesCanPullFromBelowProjection) {
  const auto parameter = correlated_parameter_(ParameterID{0}, a_a);
  const std::map<ParameterID, std::shared_ptr<AbstractExpression>> parameter_map = {{ParameterID{0}, a_a_expression}};

  // clang-format off
  const auto lqp =
  ProjectionNode::make(expression_vector(b_b),
    PredicateNode::make(less_than_(b_a, parameter),
      node_b));
  // clang-format on

  const auto result = SubqueryToJoinRule::pull_up_correlated_predicates(lqp, parameter_map);
  ASSERT_EQ(result.join_predicates.size(), 1);
  EXPECT_EQ(*result.join_predicates.front(), *greater_than_(a_a, b_a));
  // Changes to the LQP are tested in AdaptProjectionNode
}

TEST_F(SubqueryToJoinRuleTest, PullUpCorrelatedPredicatesCanPullFromBelowSort) {
  const auto parameter = correlated_parameter_(ParameterID{0}, a_a);
  const std::map<ParameterID, std::shared_ptr<AbstractExpression>> parameter_map = {{ParameterID{0}, a_a_expression}};

  // clang-format off
  const auto lqp =
  SortNode::make(expression_vector(b_a), std::vector<OrderByMode>{OrderByMode::Ascending},
    PredicateNode::make(less_than_(b_a, parameter),
      node_b));
  const auto expected_lqp =
  SortNode::make(expression_vector(b_a), std::vector<OrderByMode>{OrderByMode::Ascending},
    node_b);
  // clang-format on

  const auto result = SubqueryToJoinRule::pull_up_correlated_predicates(lqp, parameter_map);
  ASSERT_EQ(result.join_predicates.size(), 1);
  EXPECT_EQ(*result.join_predicates.front(), *greater_than_(a_a, b_a));
  EXPECT_LQP_EQ(result.adapted_lqp, expected_lqp);
}

TEST_F(SubqueryToJoinRuleTest, PullUpCorrelatedPredicatesCanPullFromBelowValidate) {
  const auto parameter = correlated_parameter_(ParameterID{0}, a_a);
  const std::map<ParameterID, std::shared_ptr<AbstractExpression>> parameter_map = {{ParameterID{0}, a_a_expression}};

  // clang-format off
  const auto lqp =
  ValidateNode::make(
    PredicateNode::make(less_than_(b_a, parameter),
      node_b));
  // clang-format on

  const auto result = SubqueryToJoinRule::pull_up_correlated_predicates(lqp, parameter_map);
  ASSERT_EQ(result.join_predicates.size(), 1);
  EXPECT_EQ(*result.join_predicates.front(), *greater_than_(a_a, b_a));
  EXPECT_LQP_EQ(result.adapted_lqp, ValidateNode::make(node_b));
}

TEST_F(SubqueryToJoinRuleTest, PullUpCorrelatedPredicatesCanPullFromBothSidesOfInnerJoin) {
  const auto parameter = correlated_parameter_(ParameterID{0}, a_a);
  const std::map<ParameterID, std::shared_ptr<AbstractExpression>> parameter_map = {{ParameterID{0}, a_a_expression}};

  // clang-format off
  const auto lqp =
  JoinNode::make(JoinMode::Inner, equals_(b_a, c_a),
    PredicateNode::make(greater_than_(b_a, parameter),
      node_b),
    PredicateNode::make(equals_(c_a, parameter),
      node_c));
  const auto expected_lqp = JoinNode::make(JoinMode::Inner, equals_(b_a, c_a), node_b, node_c);
  // clang-format on

  const auto result = SubqueryToJoinRule::pull_up_correlated_predicates(lqp, parameter_map);
  EXPECT_EQ(result.join_predicates.size(), 2);
  EXPECT_LQP_EQ(result.adapted_lqp, expected_lqp);
}

TEST_F(SubqueryToJoinRuleTest, PullUpCorrelatedPredicatesCanPullFromBothSidesOfCrossJoin) {
  const auto parameter = correlated_parameter_(ParameterID{0}, a_a);
  const std::map<ParameterID, std::shared_ptr<AbstractExpression>> parameter_map = {{ParameterID{0}, a_a_expression}};

  // clang-format off
  const auto lqp =
  JoinNode::make(JoinMode::Cross,
    PredicateNode::make(greater_than_(b_a, parameter),
      node_b),
    PredicateNode::make(equals_(c_a, parameter),
      node_c));
  const auto expected_lqp = JoinNode::make(JoinMode::Cross, node_b, node_c);
  // clang-format on

  const auto result = SubqueryToJoinRule::pull_up_correlated_predicates(lqp, parameter_map);
  EXPECT_EQ(result.join_predicates.size(), 2);
  EXPECT_LQP_EQ(result.adapted_lqp, expected_lqp);
}

TEST_F(SubqueryToJoinRuleTest, PullUpCorrelatedPredicatesCanPullFromNonNullProducingSidesOfOuterJoins) {
  const auto parameter = correlated_parameter_(ParameterID{0}, a_a);
  const std::map<ParameterID, std::shared_ptr<AbstractExpression>> parameter_map = {{ParameterID{0}, a_a_expression}};

  const auto join_predicate = equals_(b_a, c_a);
  const auto left_predicate_node = PredicateNode::make(greater_than_(b_a, parameter), node_b);
  const auto right_predicate_node = PredicateNode::make(equals_(c_a, parameter), node_c);

  // clang-format off
  const auto full_outer_lqp =
  JoinNode::make(JoinMode::FullOuter, join_predicate,
    left_predicate_node,
    right_predicate_node);
  const auto full_outer_expected_lqp = full_outer_lqp->deep_copy();
  const auto left_outer_lqp =
  JoinNode::make(JoinMode::Left, join_predicate,
    left_predicate_node,
    right_predicate_node);
  const auto left_outer_expected_lqp = JoinNode::make(JoinMode::Left, join_predicate, node_b, right_predicate_node);
  const auto right_outer_lqp =
  JoinNode::make(JoinMode::Right, join_predicate,
    left_predicate_node,
    right_predicate_node);
  const auto right_outer_expected_lqp = JoinNode::make(JoinMode::Right, join_predicate, left_predicate_node, node_c);
  // clang-format on

  const auto full_outer_result = SubqueryToJoinRule::pull_up_correlated_predicates(full_outer_lqp, parameter_map);
  EXPECT_TRUE(full_outer_result.join_predicates.empty());
  EXPECT_LQP_EQ(full_outer_result.adapted_lqp, full_outer_expected_lqp);

  const auto left_outer_result = SubqueryToJoinRule::pull_up_correlated_predicates(left_outer_lqp, parameter_map);
  ASSERT_EQ(left_outer_result.join_predicates.size(), 1);
  EXPECT_EQ(*left_outer_result.join_predicates.front(), *less_than_(a_a, b_a));
  EXPECT_LQP_EQ(left_outer_result.adapted_lqp, left_outer_expected_lqp);

  const auto right_outer_result = SubqueryToJoinRule::pull_up_correlated_predicates(right_outer_lqp, parameter_map);
  ASSERT_EQ(right_outer_result.join_predicates.size(), 1);
  EXPECT_EQ(*right_outer_result.join_predicates.front(), *equals_(a_a, c_a));
  EXPECT_LQP_EQ(right_outer_result.adapted_lqp, right_outer_expected_lqp);
}

TEST_F(SubqueryToJoinRuleTest, PullUpCorrelatedPredicatesCanPullFromLeftSideOfSemiAntiJoins) {
  const auto parameter = correlated_parameter_(ParameterID{0}, a_a);
  const std::map<ParameterID, std::shared_ptr<AbstractExpression>> parameter_map = {{ParameterID{0}, a_a_expression}};

  for (const auto join_mode : {JoinMode::Semi, JoinMode::AntiNullAsTrue, JoinMode::AntiNullAsFalse}) {
    // clang-format off
    const auto lqp =
    JoinNode::make(join_mode, equals_(b_a, c_a),
      PredicateNode::make(not_equals_(b_a, parameter),
        node_b),
      PredicateNode::make(less_than_equals_(c_a, parameter),
        node_c));
    const auto expected_lqp =
    JoinNode::make(join_mode, equals_(b_a, c_a),
      node_b,
      PredicateNode::make(less_than_equals_(c_a, parameter),
        node_c));
    // clang-format on

    const auto result = SubqueryToJoinRule::pull_up_correlated_predicates(lqp, parameter_map);
    ASSERT_EQ(result.join_predicates.size(), 1);
    EXPECT_EQ(*result.join_predicates.front(), *not_equals_(a_a, b_a));
    EXPECT_LQP_EQ(result.adapted_lqp, expected_lqp);
  }
}

TEST_F(SubqueryToJoinRuleTest, PullUpCorrelatedPredicatesCannotPullFromBelowLimits) {
  const auto parameter = correlated_parameter_(ParameterID{0}, a_a);
  const std::map<ParameterID, std::shared_ptr<AbstractExpression>> parameter_map = {{ParameterID{0}, a_a_expression}};

  // clang-format off
  const auto lqp =
  LimitNode::make(value_(1),
    PredicateNode::make(equals_(b_a, parameter),
      node_b));
  // clang-format on

  const auto result = SubqueryToJoinRule::pull_up_correlated_predicates(lqp, parameter_map);
  EXPECT_TRUE(result.join_predicates.empty());
  EXPECT_LQP_EQ(result.adapted_lqp, lqp->deep_copy());
}

TEST_F(SubqueryToJoinRuleTest, PullUpCorrelatedPredicatesRemovesPullablePredicates) {
  const auto parameter = correlated_parameter_(ParameterID{0}, a_a);
  const std::map<ParameterID, std::shared_ptr<AbstractExpression>> parameter_map = {{ParameterID{0}, a_a_expression}};

  // clang-format off
  const auto lqp =
  PredicateNode::make(equals_(b_a, parameter),
    node_b);
  // clang-format on

  const auto result = SubqueryToJoinRule::pull_up_correlated_predicates(lqp, parameter_map);
  EXPECT_LQP_EQ(result.adapted_lqp, node_b->deep_copy());
}

TEST_F(SubqueryToJoinRuleTest, PullUpCorrelatedPredicatesDoesNotChangeOriginalLQPNodes) {
  const auto parameter = correlated_parameter_(ParameterID{0}, a_a);
  const std::map<ParameterID, std::shared_ptr<AbstractExpression>> parameter_map = {{ParameterID{0}, a_a_expression}};

  // clang-format off
  const auto lqp =
  PredicateNode::make(equals_(b_a, b_b),
    PredicateNode::make(equals_(b_a, parameter),
      node_b));
  // clang-format on

  const auto lqp_copy = lqp->deep_copy();
  SubqueryToJoinRule::pull_up_correlated_predicates(lqp, parameter_map);
  EXPECT_LQP_EQ(lqp, lqp_copy);
}

TEST_F(SubqueryToJoinRuleTest, PullUpCorrelatedPredicatesHandlesDiamondLQPs) {
  const auto parameter = correlated_parameter_(ParameterID{0}, a_a);
  const std::map<ParameterID, std::shared_ptr<AbstractExpression>> parameter_map = {{ParameterID{0}, a_a_expression}};

  // clang-format off
  const auto predicate_node = PredicateNode::make(equals_(b_a, parameter), node_b);

  const auto lqp =
  JoinNode::make(JoinMode::Inner, equals_(b_b, b_b),
    ProjectionNode::make(expression_vector(b_b),
      predicate_node),
    AggregateNode::make(expression_vector(b_b), expression_vector(),
      predicate_node));

  const auto expected_lqp =
  JoinNode::make(JoinMode::Inner, equals_(b_b, b_b),
    ProjectionNode::make(expression_vector(b_b, b_a),
      node_b),
    AggregateNode::make(expression_vector(b_b, b_a), expression_vector(),
      node_b));
  // clang-format on

  const auto result = SubqueryToJoinRule::pull_up_correlated_predicates(lqp, parameter_map);
  EXPECT_LQP_EQ(result.adapted_lqp, expected_lqp);
  ASSERT_EQ(result.join_predicates.size(), 1);
}

TEST_F(SubqueryToJoinRuleTest, IsPredicateNodeJoinCandidateHandlesCorrelatedExists) {
  const auto parameter = correlated_parameter_(ParameterID{0}, a_a);
  const auto subquery_lqp = PredicateNode::make(equals_(b_a, parameter), node_b);
  const auto subquery_expression = lqp_subquery_(subquery_lqp, std::pair{ParameterID{0}, a_a});
  const auto lqp = PredicateNode::make(exists_(subquery_expression), node_a);

  const auto input_info = SubqueryToJoinRule::is_predicate_node_join_candidate(*lqp);
  ASSERT_TRUE(input_info);
  EXPECT_EQ(*input_info->subquery, *subquery_expression);
  EXPECT_EQ(input_info->join_mode, JoinMode::Semi);
  EXPECT_FALSE(input_info->join_predicate);
}

TEST_F(SubqueryToJoinRuleTest, IsPredicateNodeJoinCandidateHandlesCorrelatedNotExists) {
  const auto parameter = correlated_parameter_(ParameterID{0}, a_a);
  const auto subquery_lqp = PredicateNode::make(equals_(b_a, parameter), node_b);
  const auto subquery_expression = lqp_subquery_(subquery_lqp, std::pair{ParameterID{0}, a_a});
  const auto lqp = PredicateNode::make(not_exists_(subquery_expression), node_a);

  const auto input_info = SubqueryToJoinRule::is_predicate_node_join_candidate(*lqp);
  ASSERT_TRUE(input_info);
  EXPECT_EQ(*input_info->subquery, *subquery_expression);
  EXPECT_EQ(input_info->join_mode, JoinMode::AntiNullAsFalse);
  EXPECT_FALSE(input_info->join_predicate);
}

TEST_F(SubqueryToJoinRuleTest, IsPredicateNodeJoinCandidateHandlesIn) {
  const auto subquery_lqp = ProjectionNode::make(expression_vector(b_a), node_b);
  const auto subquery_expression = lqp_subquery_(subquery_lqp);
  const auto lqp = PredicateNode::make(in_(a_a, subquery_expression), node_a);

  const auto input_info = SubqueryToJoinRule::is_predicate_node_join_candidate(*lqp);
  ASSERT_TRUE(input_info);
  EXPECT_EQ(*input_info->subquery, *subquery_expression);
  EXPECT_EQ(input_info->join_mode, JoinMode::Semi);

  const auto mapping = lqp_create_node_mapping(subquery_expression->lqp, input_info->subquery->lqp);
  const auto copied_b_a = expression_adapt_to_different_lqp(*lqp_column_(b_a), mapping);
  EXPECT_EQ(*input_info->join_predicate, *equals_(a_a, copied_b_a));
}

TEST_F(SubqueryToJoinRuleTest, IsPredicateNodeJoinCandidateHandlesUncorrelatedNotIn) {
  const auto subquery_lqp = ProjectionNode::make(expression_vector(b_a), node_b);
  const auto subquery_expression = lqp_subquery_(subquery_lqp);
  const auto lqp = PredicateNode::make(not_in_(a_a, subquery_expression), node_a);

  const auto input_info = SubqueryToJoinRule::is_predicate_node_join_candidate(*lqp);
  ASSERT_TRUE(input_info);
  EXPECT_EQ(*input_info->subquery, *subquery_expression);
  EXPECT_EQ(input_info->join_mode, JoinMode::AntiNullAsTrue);

  const auto mapping = lqp_create_node_mapping(subquery_expression->lqp, input_info->subquery->lqp);
  const auto copied_b_a = expression_adapt_to_different_lqp(*lqp_column_(b_a), mapping);
  EXPECT_EQ(*input_info->join_predicate, *equals_(a_a, copied_b_a));
}

TEST_F(SubqueryToJoinRuleTest, IsPredicateNodeJoinCandidateHandlesComparison) {
  const auto subquery_lqp = ProjectionNode::make(expression_vector(b_a), node_b);
  const auto subquery_expression = lqp_subquery_(subquery_lqp);
  const auto lqp = PredicateNode::make(less_than_(a_a, subquery_expression), node_a);

  const auto input_info = SubqueryToJoinRule::is_predicate_node_join_candidate(*lqp);
  ASSERT_TRUE(input_info);
  EXPECT_EQ(*input_info->subquery, *subquery_expression);
  EXPECT_EQ(input_info->join_mode, JoinMode::Semi);

  const auto mapping = lqp_create_node_mapping(subquery_expression->lqp, input_info->subquery->lqp);
  const auto copied_b_a = expression_adapt_to_different_lqp(*lqp_column_(b_a), mapping);
  EXPECT_EQ(*input_info->join_predicate, *less_than_(a_a, copied_b_a));
}

TEST_F(SubqueryToJoinRuleTest, IsPredicateNodeJoinCandidateRejectsCorrelatedNotIn) {
  const auto parameter = correlated_parameter_(ParameterID{0}, a_a);

  // clang-format off
  const auto subquery_lqp =
  ProjectionNode::make(expression_vector(b_a),
    PredicateNode::make(equals_(b_a, parameter),
      node_b));
  // clang-format on

  const auto subquery_expression = lqp_subquery_(subquery_lqp, std::pair{ParameterID{0}, a_a});
  const auto lqp = PredicateNode::make(not_in_(a_a, subquery_expression), node_a);

  const auto input_info = SubqueryToJoinRule::is_predicate_node_join_candidate(*lqp);
  EXPECT_FALSE(input_info);
}

TEST_F(SubqueryToJoinRuleTest, IsPredicateNodeJoinCandidateRejectsInWithConstantList) {
  // See #1546
  const auto lqp = PredicateNode::make(in_(a_a, list_(value_(1), value_(2))), node_a);
  const auto input_info = SubqueryToJoinRule::is_predicate_node_join_candidate(*lqp);
  EXPECT_FALSE(input_info);
}

TEST_F(SubqueryToJoinRuleTest, IsPredicateNodeJoinCandidateRejectsUncorrelatedExists) {
  const auto subquery_expression = lqp_subquery_(node_b);
  for (const auto& predicate : {exists_(subquery_expression), not_exists_(subquery_expression)}) {
    const auto lqp = PredicateNode::make(predicate, node_a);
    const auto input_info = SubqueryToJoinRule::is_predicate_node_join_candidate(*lqp);
    EXPECT_FALSE(input_info);
  }
}

TEST_F(SubqueryToJoinRuleTest, IsPredicateNodeJoinCandidateLeftInOperandMustBeAColumnExpression) {
  // See #1547
  const auto subquery_lqp = ProjectionNode::make(expression_vector(b_a), node_b);
  const auto subquery_expression = lqp_subquery_(subquery_lqp);
  const auto predicate = in_(add_(a_a, 2), subquery_expression);

  const auto reject_lqp = PredicateNode::make(predicate, node_a);
  EXPECT_FALSE(SubqueryToJoinRule::is_predicate_node_join_candidate(*reject_lqp));

  // clang-format off
  const auto accept_lqp =
  PredicateNode::make(predicate,
    ProjectionNode::make(expression_vector(add_(a_a, 2)),
      node_a));
  // clang-format on

  EXPECT_TRUE(SubqueryToJoinRule::is_predicate_node_join_candidate(*accept_lqp));
}

TEST_F(SubqueryToJoinRuleTest, IsPredicateNodeJoinCandidateLeftComparisonOperandMustBeAColumnExpression) {
  // See #1547
  const auto subquery_lqp = ProjectionNode::make(expression_vector(b_a), node_b);
  const auto subquery_expression = lqp_subquery_(subquery_lqp);
  const auto predicate = less_than_(add_(a_a, 2), subquery_expression);

  const auto reject_lqp = PredicateNode::make(predicate, node_a);
  EXPECT_FALSE(SubqueryToJoinRule::is_predicate_node_join_candidate(*reject_lqp));

  // clang-format off
  const auto accept_lqp =
  PredicateNode::make(predicate,
    ProjectionNode::make(expression_vector(add_(a_a, 2)),
      node_a));
  // clang-format on

  EXPECT_TRUE(SubqueryToJoinRule::is_predicate_node_join_candidate(*accept_lqp));
}

// LQP INTEGRATION TESTS

TEST_F(SubqueryToJoinRuleTest, UncorrelatedInToSemiJoin) {
  // SELECT * FROM a WHERE a.a IN (SELECT b.a FROM b)

  // clang-format off
  const auto subquery_lqp =
  ProjectionNode::make(expression_vector(b_a),
    node_b);

  const auto subquery = lqp_subquery_(subquery_lqp);

  const auto input_lqp =
  PredicateNode::make(in_(a_a, subquery),
    node_a);

  const auto expected_lqp =
  JoinNode::make(JoinMode::Semi, equals_(a_a, b_a),
    node_a,
    ProjectionNode::make(expression_vector(b_a),
      node_b));
  // clang-format on

  const auto actual_lqp = StrategyBaseTest::apply_rule(_rule, input_lqp);

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(SubqueryToJoinRuleTest, SimpleCorrelatedInToSemiJoin) {
  // SELECT * FROM a WHERE a.a IN (SELECT b.a FROM b WHERE b.b = a.b)

  const auto parameter = correlated_parameter_(ParameterID{0}, a_b);

  // clang-format off
  const auto subquery_lqp =
  ProjectionNode::make(expression_vector(b_a),
    PredicateNode::make(equals_(b_b, parameter), node_b));

  const auto subquery = lqp_subquery_(subquery_lqp, std::make_pair(ParameterID{0}, a_b));

  const auto input_lqp =
  PredicateNode::make(in_(a_a, subquery),
    node_a);

  const auto expected_lqp =
  JoinNode::make(JoinMode::Semi, expression_vector(equals_(a_a, b_a), equals_(a_b, b_b)),
    node_a,
    ProjectionNode::make(expression_vector(b_a, b_b),
      node_b));
  // clang-format on

  const auto actual_lqp = StrategyBaseTest::apply_rule(_rule, input_lqp);

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(SubqueryToJoinRuleTest, SimpleCorrelatedExistsToSemiJoin) {
  // SELECT * FROM a WHERE EXISTS (SELECT * FROM b WHERE b.b = a.b)

  const auto parameter = correlated_parameter_(ParameterID{0}, a_b);

  // clang-format off
  const auto subquery_lqp =
  PredicateNode::make(equals_(b_b, parameter),
    node_b);

  const auto subquery = lqp_subquery_(subquery_lqp, std::make_pair(ParameterID{0}, a_b));

  const auto input_lqp =
  PredicateNode::make(exists_(subquery),
    node_a);

  const auto expected_lqp =
  JoinNode::make(JoinMode::Semi, equals_(a_b, b_b),
    node_a,
    node_b);
  // clang-format on

  const auto actual_lqp = StrategyBaseTest::apply_rule(_rule, input_lqp);

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(SubqueryToJoinRuleTest, SimpleCorrelatedExistsWithAlias) {
  // SELECT * FROM d WHERE EXISTS (SELECT e.a AS b, e.b AS a FROM e WHERE e.b = d.b)

  const auto parameter = correlated_parameter_(ParameterID{0}, d_b);

  // clang-format off
  const auto subquery_lqp =
  AliasNode::make(expression_vector(e_a, e_b), std::vector<std::string>({"b", "a"}),
    ProjectionNode::make(expression_vector(e_a),
      PredicateNode::make(equals_(e_b, parameter),
        node_e)));

  const auto subquery = lqp_subquery_(subquery_lqp, std::make_pair(ParameterID{0}, d_b));

  const auto input_lqp =
  PredicateNode::make(exists_(subquery), node_d);

  const auto expected_lqp =
  JoinNode::make(JoinMode::Semi, equals_(d_b, e_b),
    node_d,
    AliasNode::make(expression_vector(e_a, e_b), std::vector<std::string>({"b", "a"}),
      ProjectionNode::make(expression_vector(e_a, e_b),
      node_e)));
  // clang-format on

  const auto actual_lqp = StrategyBaseTest::apply_rule(_rule, input_lqp);

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(SubqueryToJoinRuleTest, DoubleCorrelatedExistsToSemiJoin) {
  // SELECT * FROM d WHERE EXISTS (SELECT * FROM e WHERE e.b = d.b AND e.c < d.c)

  const auto parameter0 = correlated_parameter_(ParameterID{0}, d_b);
  const auto parameter1 = correlated_parameter_(ParameterID{1}, d_c);

  // clang-format off
  const auto subquery_lqp =
  ProjectionNode::make(expression_vector(e_a),
    PredicateNode::make(equals_(e_b, parameter0),
      PredicateNode::make(less_than_(e_c, parameter1),
        node_e)));

  const auto subquery =
  lqp_subquery_(subquery_lqp, std::make_pair(ParameterID{0}, d_b), std::make_pair(ParameterID{1}, d_c));

  const auto input_lqp =
  PredicateNode::make(exists_(subquery),
    node_d);

  const auto expected_lqp =
  JoinNode::make(JoinMode::Semi, expression_vector(greater_than_(d_c, e_c), equals_(d_b, e_b)),
    node_d,
    ProjectionNode::make(expression_vector(e_a, e_c, e_b),
      node_e));
  // clang-format on
  const auto actual_lqp = StrategyBaseTest::apply_rule(_rule, input_lqp);

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(SubqueryToJoinRuleTest, SimpleCorrelatedInWithAdditionToSemiJoin) {
  // SELECT * FROM a WHERE a.a IN (SELECT b.a + 2 FROM b WHERE b.b = a.b)

  const auto parameter = correlated_parameter_(ParameterID{0}, a_b);

  // clang-format off
  const auto b_a_plus_2 = add_(b_a, value_(2));
  const auto subquery_lqp =
  ProjectionNode::make(expression_vector(b_a_plus_2),
    PredicateNode::make(equals_(b_b, parameter),
      node_b));

  const auto subquery = lqp_subquery_(subquery_lqp, std::make_pair(ParameterID{0}, a_b));

  const auto input_lqp =
  PredicateNode::make(in_(a_a, subquery),
    node_a);

  const auto expected_lqp =
  JoinNode::make(JoinMode::Semi, expression_vector(equals_(a_a, b_a_plus_2), equals_(a_b, b_b)),
    node_a,
    ProjectionNode::make(expression_vector(b_a_plus_2, b_b),
      node_b));
  // clang-format on

  const auto actual_lqp = StrategyBaseTest::apply_rule(_rule, input_lqp);

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(SubqueryToJoinRuleTest, UncorrelatedNestedInToSemiJoins) {
  // SELECT * FROM a WHERE a.a IN (SELECT b.a FROM b WHERE b.a IN (SELECT c.a FROM c))

  // clang-format off
  const auto inner_subquery_lqp =
  ProjectionNode::make(expression_vector(c_a),
    node_c);

  const auto inner_subquery = lqp_subquery_(inner_subquery_lqp);

  const auto subquery_lqp =
  ProjectionNode::make(expression_vector(b_a),
    PredicateNode::make(in_(b_a, inner_subquery),
      node_b));

  const auto subquery = lqp_subquery_(subquery_lqp);

  const auto input_lqp =
  PredicateNode::make(in_(a_a, subquery),
    node_a);

  const auto expected_lqp =
  JoinNode::make(JoinMode::Semi, equals_(a_a, b_a),
    node_a,
    ProjectionNode::make(expression_vector(b_a),
      JoinNode::make(JoinMode::Semi, equals_(b_a, c_a),
        node_b,
        ProjectionNode::make(expression_vector(c_a),
          node_c))));
  // clang-format on

  const auto actual_lqp = StrategyBaseTest::apply_rule(_rule, input_lqp);

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(SubqueryToJoinRuleTest, UncorrelatedNotInToAntiJoin) {
  // SELECT * FROM a WHERE a.a NOT IN (SELECT b.a FROM b)

  // clang-format off
  const auto subquery_lqp =
  ProjectionNode::make(expression_vector(b_a),
    node_b);

  const auto subquery = lqp_subquery_(subquery_lqp);

  const auto input_lqp =
  PredicateNode::make(not_in_(a_a, subquery),
    node_a);

  const auto expected_lqp =
  JoinNode::make(JoinMode::AntiNullAsTrue, equals_(a_a, b_a),
    node_a,
    ProjectionNode::make(expression_vector(b_a),
      node_b));
  // clang-format on

  const auto actual_lqp = StrategyBaseTest::apply_rule(_rule, input_lqp);

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(SubqueryToJoinRuleTest, DoubleCorrelatedInToSemiJoin) {
  // SELECT * FROM d WHERE d.a IN (SELECT e.a FROM e WHERE e.b = d.b AND e.c < d.c)

  const auto parameter0 = correlated_parameter_(ParameterID{0}, d_b);
  const auto parameter1 = correlated_parameter_(ParameterID{1}, d_c);

  // clang-format off
  const auto subquery_lqp =
  ProjectionNode::make(expression_vector(e_a),
    PredicateNode::make(equals_(e_b, parameter0),
      PredicateNode::make(less_than_(e_c, parameter1),
        node_e)));

  const auto subquery =
  lqp_subquery_(subquery_lqp, std::make_pair(ParameterID{0}, d_b), std::make_pair(ParameterID{1}, d_c));

  const auto input_lqp =
  PredicateNode::make(in_(d_a, subquery),
    node_d);

  const auto join_predicates = expression_vector(equals_(d_a, e_a), greater_than_(d_c, e_c), equals_(d_b, e_b));

  const auto expected_lqp =
  JoinNode::make(JoinMode::Semi, join_predicates,
    node_d,
    ProjectionNode::make(expression_vector(e_a, e_c, e_b),
      node_e));
  // clang-format on

  const auto actual_lqp = StrategyBaseTest::apply_rule(_rule, input_lqp);

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(SubqueryToJoinRuleTest, UncorrelatedComparatorToSemiJoin) {
  // SELECT * FROM a WHERE a.a = (SELECT SUM(b.a) FROM b)

  const auto parameter = correlated_parameter_(ParameterID{0}, a_b);

  // clang-format off
  const auto subquery_lqp =
  AggregateNode::make(expression_vector(), expression_vector(sum_(b_a)),
    node_b);

  const auto subquery = lqp_subquery_(subquery_lqp, std::make_pair(ParameterID{0}, a_b));

  const auto input_lqp =
  PredicateNode::make(equals_(a_a, subquery),
    node_a);

  const auto expected_lqp =
  JoinNode::make(JoinMode::Semi, expression_vector(equals_(a_a, sum_(b_a))),
    node_a,
    AggregateNode::make(expression_vector(), expression_vector(sum_(b_a)),
      node_b));
  // clang-format on

  const auto actual_lqp = StrategyBaseTest::apply_rule(_rule, input_lqp);

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(SubqueryToJoinRuleTest, SimpleCorrelatedComparatorToSemiJoin) {
  // SELECT * FROM a WHERE a.a > (SELECT SUM(b.a) FROM b WHERE b.b = a.b)

  const auto parameter = correlated_parameter_(ParameterID{0}, a_b);

  // clang-format off
  const auto subquery_lqp =
  AggregateNode::make(expression_vector(), expression_vector(sum_(b_a)),
    PredicateNode::make(equals_(b_b, parameter),
      node_b));

  const auto subquery = lqp_subquery_(subquery_lqp, std::make_pair(ParameterID{0}, a_b));

  const auto input_lqp =
  PredicateNode::make(greater_than_(a_a, subquery),
    node_a);

  const auto expected_lqp =
  JoinNode::make(JoinMode::Semi, expression_vector(greater_than_(a_a, sum_(b_a)), equals_(a_b, b_b)),
    node_a,
    AggregateNode::make(expression_vector(b_b), expression_vector(sum_(b_a)),
      node_b));
  // clang-format on

  const auto actual_lqp = StrategyBaseTest::apply_rule(_rule, input_lqp);

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(SubqueryToJoinRuleTest, DoubleCorrelatedComparatorToSemiJoin) {
  // SELECT * FROM d WHERE d.a > (SELECT SUM(e.a) FROM e WHERE e.b = d.b AND e.c = d.c)

  const auto parameter0 = correlated_parameter_(ParameterID{0}, d_b);
  const auto parameter1 = correlated_parameter_(ParameterID{1}, d_c);

  // clang-format off
  const auto subquery_lqp =
  AggregateNode::make(expression_vector(), expression_vector(sum_(e_a)),
    PredicateNode::make(equals_(e_b, parameter0),
      PredicateNode::make(equals_(e_c, parameter1),
        node_e)));

  const auto subquery =
  lqp_subquery_(subquery_lqp, std::make_pair(ParameterID{0}, d_b), std::make_pair(ParameterID{1}, d_c));

  const auto input_lqp =
  PredicateNode::make(greater_than_(d_a, subquery),
    node_d);

  const auto join_predicates = expression_vector(greater_than_(d_a, sum_(e_a)), equals_(d_c, e_c), equals_(d_b, e_b));

  const auto expected_lqp =
  JoinNode::make(JoinMode::Semi, join_predicates,
    node_d,
    AggregateNode::make(expression_vector(e_c, e_b), expression_vector(sum_(e_a)),
      node_e));
  // clang-format on

  const auto actual_lqp = StrategyBaseTest::apply_rule(_rule, input_lqp);

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(SubqueryToJoinRuleTest, MultipliedCorrelatedComparatorToSemiJoin) {
  // clang-format off
  // SELECT * FROM a WHERE a.a > 3 / (SELECT SUM(b.a) FROM b WHERE b.b = a.b)
  {
    const auto parameter = correlated_parameter_(ParameterID{0}, a_b);
    const auto subquery_lqp =
    AggregateNode::make(expression_vector(), expression_vector(sum_(b_a)),
      PredicateNode::make(equals_(b_b, parameter),
        node_b));
    const auto subquery = lqp_subquery_(subquery_lqp, std::make_pair(ParameterID{0}, a_b));
    const auto input_lqp =
    PredicateNode::make(greater_than_(a_a, div_(value_(3), subquery)),
      node_a);

    const auto expected_lqp =
    JoinNode::make(JoinMode::Semi, expression_vector(greater_than_(a_a, div_(value_(3), sum_(b_a))), equals_(a_b, b_b)),
      node_a,
      ProjectionNode::make(expression_vector(div_(value_(3), sum_(b_a)), b_b),
        AggregateNode::make(expression_vector(b_b), expression_vector(sum_(b_a)),
          node_b)));

    const auto actual_lqp = StrategyBaseTest::apply_rule(_rule, input_lqp);
    EXPECT_LQP_EQ(actual_lqp, expected_lqp);
  }

  // SELECT * FROM a WHERE a.a > (SELECT SUM(b.a) FROM b WHERE b.b = a.b) / 3
  {
    const auto parameter = correlated_parameter_(ParameterID{0}, a_b);
    const auto subquery_lqp =
    AggregateNode::make(expression_vector(), expression_vector(sum_(b_a)),
      PredicateNode::make(equals_(b_b, parameter),
        node_b));
    const auto subquery = lqp_subquery_(subquery_lqp, std::make_pair(ParameterID{0}, a_b));
    const auto input_lqp =
    PredicateNode::make(greater_than_(a_a, div_(subquery, value_(3))),
      node_a);

    const auto expected_lqp =
    JoinNode::make(JoinMode::Semi, expression_vector(greater_than_(a_a, div_(sum_(b_a), value_(3))), equals_(a_b, b_b)),
      node_a,
      ProjectionNode::make(expression_vector(div_(sum_(b_a), value_(3)), b_b),
        AggregateNode::make(expression_vector(b_b), expression_vector(sum_(b_a)),
          node_b)));

    const auto actual_lqp = StrategyBaseTest::apply_rule(_rule, input_lqp);
    EXPECT_LQP_EQ(actual_lqp, expected_lqp);
  }

  // SELECT * FROM a WHERE 3 / (SELECT SUM(b.a) FROM b WHERE b.b = a.b) < a.a
  {
    const auto parameter = correlated_parameter_(ParameterID{0}, a_b);
    const auto subquery_lqp =
    AggregateNode::make(expression_vector(), expression_vector(sum_(b_a)),
      PredicateNode::make(equals_(b_b, parameter),
        node_b));
    const auto subquery = lqp_subquery_(subquery_lqp, std::make_pair(ParameterID{0}, a_b));
    const auto input_lqp =
    PredicateNode::make(less_than_(div_(value_(3), subquery), a_a),
      node_a);

    const auto expected_lqp =
    JoinNode::make(JoinMode::Semi, expression_vector(greater_than_(a_a, div_(value_(3), sum_(b_a))), equals_(a_b, b_b)),
      node_a,
      ProjectionNode::make(expression_vector(div_(value_(3), sum_(b_a)), b_b),
        AggregateNode::make(expression_vector(b_b), expression_vector(sum_(b_a)),
          node_b)));

    const auto actual_lqp = StrategyBaseTest::apply_rule(_rule, input_lqp);
    EXPECT_LQP_EQ(actual_lqp, expected_lqp);
  }

  // SELECT * FROM a WHERE (SELECT SUM(b.a) FROM b WHERE b.b = a.b) / 3 < a.a
  {
    const auto parameter = correlated_parameter_(ParameterID{0}, a_b);
    const auto subquery_lqp =
    AggregateNode::make(expression_vector(), expression_vector(sum_(b_a)),
      PredicateNode::make(equals_(b_b, parameter),
        node_b));
    const auto subquery = lqp_subquery_(subquery_lqp, std::make_pair(ParameterID{0}, a_b));
    const auto input_lqp =
    PredicateNode::make(less_than_(div_(subquery, value_(3)), a_a),
      node_a);

    const auto expected_lqp =
    JoinNode::make(JoinMode::Semi, expression_vector(greater_than_(a_a, div_(sum_(b_a), value_(3))), equals_(a_b, b_b)),
      node_a,
      ProjectionNode::make(expression_vector(div_(sum_(b_a), value_(3)), b_b),
        AggregateNode::make(expression_vector(b_b), expression_vector(sum_(b_a)),
          node_b)));

    const auto actual_lqp = StrategyBaseTest::apply_rule(_rule, input_lqp);
    EXPECT_LQP_EQ(actual_lqp, expected_lqp);
  }
  // clang-format on
}

TEST_F(SubqueryToJoinRuleTest, SubqueryUsesConjunctionOfCorrelatedAndLocalPredicates) {
  // SELECT * FROM d WHERE d.a > (SELECT SUM(e.a) FROM e WHERE e.b = d.b AND e.a IN (1, 2, 3))

  const auto parameter0 = correlated_parameter_(ParameterID{0}, d_b);

  // clang-format off
  const auto subquery_lqp =
  AggregateNode::make(expression_vector(), expression_vector(sum_(e_a)),
    PredicateNode::make(and_(equals_(e_b, parameter0), in_(e_a, list_(1, 2, 3))),
      node_e));

  const auto subquery = lqp_subquery_(subquery_lqp, std::make_pair(ParameterID{0}, d_b));

  const auto input_lqp =
  PredicateNode::make(greater_than_(d_a, subquery),
    node_d);

  const auto join_predicates = expression_vector(greater_than_(d_a, sum_(e_a)), equals_(d_b, e_b));

  const auto expected_lqp =
  JoinNode::make(JoinMode::Semi, join_predicates,
    node_d,
    AggregateNode::make(expression_vector(e_b), expression_vector(sum_(e_a)),
      PredicateNode::make(in_(e_a, list_(1, 2, 3)),
        node_e)));
  // clang-format on

  const auto actual_lqp = StrategyBaseTest::apply_rule(_rule, input_lqp);

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(SubqueryToJoinRuleTest, OptimizeTPCH17) {
  // Optimize a pre-optimized LQP version of TPC-H 17, as the SubqueryToJoinRule would receive it

  const auto parameter = correlated_parameter_(ParameterID{0}, p_partkey);

  // clang-format off
  const auto subquery_lqp =
  ProjectionNode::make(expression_vector(mul_(value_(0.2), avg_(l_quantity))),
    AggregateNode::make(expression_vector(), expression_vector(avg_(l_quantity)),
      PredicateNode::make(equals_(l_partkey, parameter),
        ProjectionNode::make(expression_vector(l_partkey, l_quantity),
          lineitem))));

  const auto subquery = lqp_subquery_(subquery_lqp, std::make_pair(ParameterID{0}, p_partkey));

  const auto input_lqp =
  AliasNode::make(expression_vector(div_(sum_(l_extendedprice), value_(7))), std::vector<std::string>{"avg_yearly"},
    ProjectionNode::make(expression_vector(div_(sum_(l_extendedprice), value_(7))),
      AggregateNode::make(expression_vector(), expression_vector(sum_(l_extendedprice)),
        PredicateNode::make(less_than_(l_quantity, subquery),
          JoinNode::make(JoinMode::Inner, equals_(p_partkey, l_partkey),
            ProjectionNode::make(expression_vector(l_partkey, l_quantity, l_extendedprice),
              lineitem),
            PredicateNode::make(equals_(p_container, value_("test_container")),
              PredicateNode::make(equals_(p_brand, value_("test_brand")),
                ProjectionNode::make(expression_vector(p_partkey, p_brand, p_container),
                  part))))))));

  const auto expected_lqp =
  AliasNode::make(expression_vector(div_(sum_(l_extendedprice), value_(7))), std::vector<std::string>{"avg_yearly"},
    ProjectionNode::make(expression_vector(div_(sum_(l_extendedprice), value_(7))),
      AggregateNode::make(expression_vector(), expression_vector(sum_(l_extendedprice)),
        JoinNode::make(JoinMode::Semi, expression_vector(less_than_(l_quantity, mul_(value_(0.2), avg_(l_quantity))),
                                                         equals_(p_partkey, l_partkey)),
          JoinNode::make(JoinMode::Inner, equals_(p_partkey, l_partkey),
            ProjectionNode::make(expression_vector(l_partkey, l_quantity, l_extendedprice),
              lineitem),
            PredicateNode::make(equals_(p_container, value_("test_container")),
              PredicateNode::make(equals_(p_brand, value_("test_brand")),
                ProjectionNode::make(expression_vector(p_partkey, p_brand, p_container),
                  part)))),
          ProjectionNode::make(expression_vector(mul_(value_(0.2), avg_(l_quantity)), l_partkey),
            AggregateNode::make(expression_vector(l_partkey), expression_vector(avg_(l_quantity)),
              ProjectionNode::make(expression_vector(l_partkey, l_quantity),
                lineitem)))))));
  // clang-format on

  const auto actual_lqp = StrategyBaseTest::apply_rule(_rule, input_lqp);

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(SubqueryToJoinRuleTest, OptimizeTPCH20) {
  // Optimize a pre-optimized LQP version of TPC-H 20, as the SubqueryToJoinRule would receive it

  const auto parameter0 = correlated_parameter_(ParameterID{0}, ps_partkey);
  const auto parameter1 = correlated_parameter_(ParameterID{1}, ps_suppkey);

  // clang-format off
  const auto subquery_lqp0 =
  ProjectionNode::make(expression_vector(mul_(value_(0.5), sum_(l_quantity))),
    PredicateNode::make(less_than_(l_shipdate, "01.01.2020"),
      PredicateNode::make(greater_than_equals_(l_shipdate, "01.01.2019"),
        PredicateNode::make(equals_(l_suppkey, parameter1),
          PredicateNode::make(equals_(l_partkey, parameter0),
            ProjectionNode::make(expression_vector(l_partkey, l_suppkey, l_quantity, l_shipdate),
              lineitem))))));

  const auto subquery0 =
  lqp_subquery_(subquery_lqp0, std::make_pair(ParameterID{0}, ps_partkey), std::make_pair(ParameterID{1}, ps_suppkey));

  const auto subquery_lqp1 =
  ProjectionNode::make(expression_vector(p_partkey),
    PredicateNode::make(like_(p_name, "test_color%"),
      ProjectionNode::make(expression_vector(p_partkey, p_name),
        part)));

  const auto subquery1 = lqp_subquery_(subquery_lqp1);

  const auto subquery_lqp2 =
  ProjectionNode::make(expression_vector(ps_suppkey),
    PredicateNode::make(greater_than_(ps_availqty, subquery0),
      PredicateNode::make(in_(ps_partkey, subquery1),
        ProjectionNode::make(expression_vector(ps_partkey, ps_suppkey, ps_availqty),
         partsupp))));

  const auto subquery2 = lqp_subquery_(subquery_lqp2);

  const auto input_lqp =
  ProjectionNode::make(expression_vector(s_name, s_address),
    SortNode::make(expression_vector(s_name), std::vector<OrderByMode>{OrderByMode::Ascending},
      JoinNode::make(JoinMode::Inner, equals_(s_nationkey, n_nationkey),
        PredicateNode::make(in_(s_suppkey, subquery2),
          ProjectionNode::make(expression_vector(s_suppkey, s_name, s_address, s_nationkey),
            supplier)),
        PredicateNode::make(equals_(n_name, "test_nation"),
          ProjectionNode::make(expression_vector(n_nationkey, n_name),
            nation)))));

  const auto join_predicates = expression_vector(
    greater_than_(ps_availqty, mul_(value_(0.5), sum_(l_quantity))),
    equals_(ps_partkey, l_partkey),
    equals_(ps_suppkey, l_suppkey));

  const auto expected_lqp =
  ProjectionNode::make(expression_vector(s_name, s_address),
    SortNode::make(expression_vector(s_name), std::vector<OrderByMode>{OrderByMode::Ascending},
      JoinNode::make(JoinMode::Inner, equals_(s_nationkey, n_nationkey),
        JoinNode::make(JoinMode::Semi, equals_(s_suppkey, ps_suppkey),
          ProjectionNode::make(expression_vector(s_suppkey, s_name, s_address, s_nationkey),
            supplier),
          ProjectionNode::make(expression_vector(ps_suppkey),
            JoinNode::make(JoinMode::Semi, join_predicates,
              JoinNode::make(JoinMode::Semi, equals_(ps_partkey, p_partkey),
                ProjectionNode::make(expression_vector(ps_partkey, ps_suppkey, ps_availqty),
                  partsupp),
                ProjectionNode::make(expression_vector(p_partkey),
                  PredicateNode::make(like_(p_name, "test_color%"),
                    ProjectionNode::make(expression_vector(p_partkey, p_name),
                      part)))),
              ProjectionNode::make(expression_vector(mul_(value_(0.5), sum_(l_quantity)), l_partkey, l_suppkey),
                PredicateNode::make(less_than_(l_shipdate, "01.01.2020"),
                  PredicateNode::make(greater_than_equals_(l_shipdate, "01.01.2019"),
                    ProjectionNode::make(expression_vector(l_partkey, l_suppkey, l_quantity, l_shipdate),
                      lineitem))))))),
        PredicateNode::make(equals_(n_name, "test_nation"),
          ProjectionNode::make(expression_vector(n_nationkey, n_name),
            nation)))));
  // clang-format on

  const auto actual_lqp = StrategyBaseTest::apply_rule(_rule, input_lqp);

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

// The reformulation requires OR support in the MultiPredicateJoinOperator (#1580).
TEST_F(SubqueryToJoinRuleTest, NoRewriteOfOr) {
  // SELECT * FROM d WHERE d.a IN (SELECT e.a FROM e WHERE e.b = d.b OR e.c < d.c)

  const auto parameter0 = correlated_parameter_(ParameterID{0}, d_b);
  const auto parameter1 = correlated_parameter_(ParameterID{1}, d_c);

  // clang-format off
  const auto subquery_lqp =
  ProjectionNode::make(expression_vector(e_a),
    PredicateNode::make(or_(equals_(e_b, parameter0), less_than_(e_c, parameter1)),
      node_e));

  const auto subquery =
  lqp_subquery_(subquery_lqp, std::make_pair(ParameterID{0}, d_b), std::make_pair(ParameterID{1}, d_c));

  const auto input_lqp =
  PredicateNode::make(in_(d_a, subquery),
    node_d);

  const auto expected_lqp = input_lqp->deep_copy();
  // clang-format on

  const auto actual_lqp = StrategyBaseTest::apply_rule(_rule, input_lqp);

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(SubqueryToJoinRuleTest, NoRewriteConstantIn) {
  // SELECT * FROM a WHERE IN (1, 2, 3)

  // clang-format off
  const auto input_lqp =
  PredicateNode::make(in_(a_a, list_(1, 2, 3)),
    node_a);

  const auto expected_lqp = input_lqp->deep_copy();
  // clang-format on

  const auto actual_lqp = StrategyBaseTest::apply_rule(_rule, input_lqp);

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(SubqueryToJoinRuleTest, NoRewriteUncorrelatedExists) {
  // SELECT * FROM a WHERE (NOT) EXISTS (SELECT * FROM b)

  const auto subquery = lqp_subquery_(node_b);

  std::vector<std::shared_ptr<ExistsExpression>> predicates;
  predicates.emplace_back(exists_(subquery));
  predicates.emplace_back(not_exists_(subquery));

  for (const auto& predicate : predicates) {
    // clang-format off
    const auto input_lqp =
    PredicateNode::make(predicate,
      node_a);

    const auto expected_lqp = input_lqp->deep_copy();
    // clang-format on

    const auto actual_lqp = StrategyBaseTest::apply_rule(_rule, input_lqp);

    EXPECT_LQP_EQ(actual_lqp, expected_lqp);
  }
}

TEST_F(SubqueryToJoinRuleTest, NoRewriteCorrelatedNotIn) {
  // SELECT * FROM a WHERE a.a NOT IN (SELECT b.a FROM b WHERE b.b = a.b)

  const auto parameter = correlated_parameter_(ParameterID{0}, a_b);

  // clang-format off
  const auto subquery_lqp =
  ProjectionNode::make(expression_vector(b_a),
    PredicateNode::make(equals_(b_b, parameter),
      node_b));

  const auto subquery = lqp_subquery_(subquery_lqp, std::make_pair(ParameterID{0}, a_b));

  const auto input_lqp =
  PredicateNode::make(not_in_(a_a, subquery),
    node_a);

  const auto expected_lqp = input_lqp->deep_copy();
  // clang-format on

  const auto actual_lqp = StrategyBaseTest::apply_rule(_rule, input_lqp);

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

// The reformulation requires semi-/anti-join support in the SortMergeJoin operator (#1497).
TEST_F(SubqueryToJoinRuleTest, NoRewriteIfNoEqualsPredicateCanBeDerived) {
  // SELECT * FROM a WHERE EXISTS (SELECT * FROM b WHERE b.b < a.b)

  const auto parameter = correlated_parameter_(ParameterID{0}, a_b);

  // clang-format off
  const auto subquery_lqp =
  PredicateNode::make(less_than_(b_b, parameter),
    node_b);

  const auto subquery = lqp_subquery_(subquery_lqp, std::make_pair(ParameterID{0}, a_b));

  const auto input_lqp =
  PredicateNode::make(exists_(subquery),
    node_a);

  const auto expected_lqp = input_lqp->deep_copy();
  // clang-format on

  const auto actual_lqp = StrategyBaseTest::apply_rule(_rule, input_lqp);

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(SubqueryToJoinRuleTest, ComplexArithmeticExpression) {
  // SELECT * FROM a WHERE a.a > (SELECT SUM(a.a) FROM a WHERE a.b = a.b) + (SELECT SUM(b.a) FROM b WHERE b.b = a.b)

  // clang-format off
  const auto parameter = correlated_parameter_(ParameterID{0}, a_b);

  const auto subquery_lqp0 =
  AggregateNode::make(expression_vector(), expression_vector(sum_(a_a)),
    PredicateNode::make(equals_(a_b, parameter),
      node_a));
  const auto subquery0 = lqp_subquery_(subquery_lqp0, std::make_pair(ParameterID{0}, a_b));

  const auto subquery_lqp1 =
  AggregateNode::make(expression_vector(), expression_vector(sum_(b_a)),
    PredicateNode::make(equals_(b_b, parameter),
      node_b));
  const auto subquery1 = lqp_subquery_(subquery_lqp1, std::make_pair(ParameterID{0}, a_b));

  const auto input_lqp =
  PredicateNode::make(greater_than_(a_a, add_(subquery0, subquery1)),
    node_a);


  const auto subquery_lqp2 =
  ProjectionNode::make(expression_vector(add_(sum_(a_a), subquery1)),
    AggregateNode::make(expression_vector(), expression_vector(sum_(a_a)),
      PredicateNode::make(equals_(a_b, parameter),
        node_a)));
  const auto subquery2 = lqp_subquery_(subquery_lqp2, std::make_pair(ParameterID{0}, a_b));


  const auto expected_lqp =
  PredicateNode::make(greater_than_(a_a, subquery2),
    node_a);
  // clang-format on

  const auto actual_lqp = StrategyBaseTest::apply_rule(_rule, input_lqp);
  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

}  // namespace opossum
