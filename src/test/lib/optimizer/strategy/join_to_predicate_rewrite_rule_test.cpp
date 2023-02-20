#include "strategy_base_test.hpp"

#include "magic_enum.hpp"

#include "expression/expression_functional.hpp"
#include "logical_query_plan/aggregate_node.hpp"
#include "logical_query_plan/change_meta_table_node.hpp"
#include "logical_query_plan/delete_node.hpp"
#include "logical_query_plan/export_node.hpp"
#include "logical_query_plan/insert_node.hpp"
#include "logical_query_plan/join_node.hpp"
#include "logical_query_plan/mock_node.hpp"
#include "logical_query_plan/predicate_node.hpp"
#include "logical_query_plan/projection_node.hpp"
#include "logical_query_plan/sort_node.hpp"
#include "logical_query_plan/stored_table_node.hpp"
#include "logical_query_plan/union_node.hpp"
#include "logical_query_plan/update_node.hpp"
#include "logical_query_plan/validate_node.hpp"
#include "optimizer/strategy/column_pruning_rule.hpp"
#include "optimizer/strategy/join_to_predicate_rewrite_rule.hpp"

namespace hyrise {

using namespace expression_functional;  // NOLINT(build/namespaces)

class JoinToPredicateRewriteRuleTest : public StrategyBaseTest {
 public:
  void SetUp() override {
    node_a = MockNode::make(
        MockNode::ColumnDefinitions{{DataType::Int, "a"}, {DataType::Int, "b"}, {DataType::Int, "c"}}, "a");
    node_b = MockNode::make(
        MockNode::ColumnDefinitions{{DataType::Int, "u"}, {DataType::Int, "v"}, {DataType::Int, "w"}}, "b");
    table_c = Table::create_dummy_table(
        {{"x", DataType::Int, false}, {"y", DataType::Int, false}, {"z", DataType::Int, false}});
    Hyrise::get().storage_manager.add_table("table_c", table_c);
    node_c = StoredTableNode::make("table_c");

    a = node_a->get_column("a");
    b = node_a->get_column("b");
    c = node_a->get_column("c");
    u = node_b->get_column("u");
    v = node_b->get_column("v");
    w = node_b->get_column("w");
    x = node_c->get_column("x");
    y = node_c->get_column("y");
    z = node_c->get_column("z");

    rule = std::make_shared<JoinToPredicateRewriteRule>();
  }

  std::shared_ptr<JoinToPredicateRewriteRule> rule;
  std::shared_ptr<MockNode> node_a, node_b;
  std::shared_ptr<StoredTableNode> node_c;
  std::shared_ptr<LQPColumnExpression> a, b, c, u, v, w, x, y, z;
  std::shared_ptr<Table> table_c;
};

class JoinToPredicateRewriteRuleJoinModeTest : public JoinToPredicateRewriteRuleTest,
                                               public ::testing::WithParamInterface<JoinMode> {};

INSTANTIATE_TEST_SUITE_P(JoinToPredicateRewriteRuleJoinModeTestInstance, JoinToPredicateRewriteRuleJoinModeTest,
                         ::testing::ValuesIn(magic_enum::enum_values<JoinMode>()), enum_formatter<JoinMode>);

TEST_P(JoinToPredicateRewriteRuleJoinModeTest, PerformUccRewrite) {
  // The rule should only rewrite inner and semi joins.
  const auto key_constraint_u = TableKeyConstraint{{u->original_column_id}, KeyConstraintType::UNIQUE};
  const auto key_constraint_v = TableKeyConstraint{{v->original_column_id}, KeyConstraintType::UNIQUE};
  node_b->set_key_constraints({key_constraint_u, key_constraint_v});

  const auto join_node =
      GetParam() == JoinMode::Cross ? JoinNode::make(GetParam()) : JoinNode::make(GetParam(), equals_(a, u));
  join_node->set_left_input(node_a);

  // clang-format off
  const auto right_input =
  ValidateNode::make(
    PredicateNode::make(equals_(v, 0),
      node_b));
  join_node->set_right_input(right_input);

  const auto lqp =
  ProjectionNode::make(expression_vector(b),
    join_node);

  const auto subquery =
  ProjectionNode::make(expression_vector(u),
    ValidateNode::make(
      PredicateNode::make(equals_(v, 0),
        node_b)));

  auto expected_lqp =
  ProjectionNode::make(expression_vector(b),
    PredicateNode::make(equals_(a, lqp_subquery_(subquery)),
      node_a));
  // clang-format on

  const auto annotated_lqp = apply_rule(std::make_shared<ColumnPruningRule>(), lqp->deep_copy());
  const auto actual_lqp = apply_rule(rule, annotated_lqp);
  expected_lqp = std::static_pointer_cast<ProjectionNode>(
      apply_rule(std::make_shared<ColumnPruningRule>(), expected_lqp->deep_copy()));

  if (GetParam() == JoinMode::Inner || GetParam() == JoinMode::Semi) {
    EXPECT_LQP_EQ(actual_lqp, expected_lqp);
  } else {
    EXPECT_LQP_EQ(actual_lqp, annotated_lqp->deep_copy());
  }
}

TEST_P(JoinToPredicateRewriteRuleJoinModeTest, PerformOdRewritePredicate) {
  // The rule should only rewrite inner and semi joins.
  const auto key_constraint = TableKeyConstraint{{u->original_column_id}, KeyConstraintType::UNIQUE};
  const auto order_constraint = TableOrderConstraint{{u->original_column_id}, {v->original_column_id}};
  const auto foreign_key_constraint =
      ForeignKeyConstraint{{u->original_column_id}, {x->original_column_id}, nullptr, table_c};
  node_b->set_key_constraints({key_constraint});
  node_b->set_order_constraints({order_constraint});
  node_b->set_foreign_key_constraints({foreign_key_constraint});

  const auto predicates = std::vector<std::shared_ptr<AbstractExpression>>{
      equals_(v, 0), between_inclusive_(v, 0, 100), between_lower_exclusive_(v, 0, 100),
      between_upper_exclusive_(v, 0, 100), between_exclusive_(v, 0, 100)};

  for (const auto& predicate : predicates) {
    const auto join_node =
        GetParam() == JoinMode::Cross ? JoinNode::make(GetParam()) : JoinNode::make(GetParam(), equals_(x, u));
    join_node->set_left_input(node_c);

    // clang-format off
    const auto right_input =
    ValidateNode::make(
      PredicateNode::make(predicate,
        node_b));
    join_node->set_right_input(right_input);

    const auto lqp =
    ProjectionNode::make(expression_vector(y),
      join_node);

    const auto aggregate_node =
    AggregateNode::make(expression_vector(), expression_vector(min_(u), max_(u)),
      ValidateNode::make(
        PredicateNode::make(predicate,
          node_b)));

    const auto subquery_min =
    ProjectionNode::make(expression_vector(min_(u)),
      aggregate_node);

    const auto subquery_max =
    ProjectionNode::make(expression_vector(max_(u)),
      aggregate_node);

    auto expected_lqp =
    ProjectionNode::make(expression_vector(y),
      PredicateNode::make(between_inclusive_(x, lqp_subquery_(subquery_min), lqp_subquery_(subquery_max)),
        node_c));
    // clang-format on

    const auto annotated_lqp = apply_rule(std::make_shared<ColumnPruningRule>(), lqp->deep_copy());
    const auto actual_lqp = apply_rule(rule, annotated_lqp);
    expected_lqp = std::static_pointer_cast<ProjectionNode>(
        apply_rule(std::make_shared<ColumnPruningRule>(), expected_lqp->deep_copy()));

    SCOPED_TRACE("For predicate " + predicate->description());
    if (GetParam() == JoinMode::Inner || GetParam() == JoinMode::Semi) {
      EXPECT_LQP_EQ(actual_lqp, expected_lqp);
    } else {
      EXPECT_LQP_EQ(actual_lqp, annotated_lqp->deep_copy());
    }
  }
}

TEST_F(JoinToPredicateRewriteRuleTest, MissingPredicate) {
  // Do not rewrite if there is no predicate on the column with UCC or OD.
  const auto key_constraint_u = TableKeyConstraint{{u->original_column_id}, KeyConstraintType::UNIQUE};
  const auto key_constraint_v = TableKeyConstraint{{v->original_column_id}, KeyConstraintType::UNIQUE};
  node_b->set_key_constraints({key_constraint_u, key_constraint_v});
  const auto order_constraint = TableOrderConstraint{{ColumnID{0}}, {ColumnID{1}}};
  node_b->set_order_constraints({order_constraint});
  const auto foreign_key_constraint =
      ForeignKeyConstraint{{u->original_column_id}, {x->original_column_id}, nullptr, table_c};
  node_b->set_foreign_key_constraints({foreign_key_constraint});

  // clang-format off
  const auto lqp =
  ProjectionNode::make(expression_vector(y),
    JoinNode::make(JoinMode::Inner, equals_(x, u),
      node_c,
      node_b));
  // clang-format on

  const auto annotated_lqp = apply_rule(std::make_shared<ColumnPruningRule>(), lqp);
  const auto actual_lqp = apply_rule(rule, annotated_lqp);
  EXPECT_LQP_EQ(actual_lqp, lqp->deep_copy());
}

TEST_F(JoinToPredicateRewriteRuleTest, MissingDependencyOnBetweenPredicateColumn) {
  // Do not rewrite if there is no UCC or OD on the predicate column.
  const auto key_constraint_u = TableKeyConstraint{{u->original_column_id}, KeyConstraintType::UNIQUE};
  node_b->set_key_constraints({key_constraint_u});
  const auto foreign_key_constraint =
      ForeignKeyConstraint{{u->original_column_id}, {x->original_column_id}, nullptr, table_c};
  node_b->set_foreign_key_constraints({foreign_key_constraint});

  // clang-format off
  const auto lqp =
  ProjectionNode::make(expression_vector(y),
    JoinNode::make(JoinMode::Semi, equals_(x, u),
      node_c,
      PredicateNode::make(between_inclusive_(v, 0, 100),
        node_b)));
  // clang-format on

  const auto annotated_lqp = apply_rule(std::make_shared<ColumnPruningRule>(), lqp);
  const auto actual_lqp = apply_rule(rule, annotated_lqp);
  EXPECT_LQP_EQ(actual_lqp, lqp->deep_copy());
}

TEST_F(JoinToPredicateRewriteRuleTest, MissingIndForOdRewrite) {
  // Do not rewrite if there is no predicate on the column with UCC or OD.
  const auto key_constraint_u = TableKeyConstraint{{u->original_column_id}, KeyConstraintType::UNIQUE};
  const auto key_constraint_v = TableKeyConstraint{{v->original_column_id}, KeyConstraintType::UNIQUE};
  node_b->set_key_constraints({key_constraint_u, key_constraint_v});
  const auto order_constraint = TableOrderConstraint{{ColumnID{0}}, {ColumnID{1}}};
  node_b->set_order_constraints({order_constraint});

  for (const auto& predicate :
       std::vector<std::shared_ptr<AbstractExpression>>{equals_(v, 0), between_inclusive_(v, 0, 100)}) {
    node_b->set_pruned_column_ids({});
    node_c->set_pruned_column_ids({});

    // clang-format off
    const auto lqp =
    ProjectionNode::make(expression_vector(y),
      JoinNode::make(JoinMode::Inner, equals_(x, u),
        node_c,
        PredicateNode::make(predicate,
          node_b)));
    // clang-format on

    const auto annotated_lqp = apply_rule(std::make_shared<ColumnPruningRule>(), lqp);
    const auto actual_lqp = apply_rule(rule, annotated_lqp);
    SCOPED_TRACE("For predicate " + predicate->description());
    EXPECT_LQP_EQ(actual_lqp, lqp->deep_copy());
  }
}

TEST_F(JoinToPredicateRewriteRuleTest, MissingUccOnJoinColumn) {
  // Do not rewrite if there is no UCC on the join column.
  const auto key_constraint_v = TableKeyConstraint{{v->original_column_id}, KeyConstraintType::UNIQUE};
  node_b->set_key_constraints({key_constraint_v});
  const auto foreign_key_constraint =
      ForeignKeyConstraint{{u->original_column_id}, {x->original_column_id}, nullptr, table_c};
  node_b->set_foreign_key_constraints({foreign_key_constraint});

  for (const auto& predicate :
       std::vector<std::shared_ptr<AbstractExpression>>{equals_(v, 0), between_inclusive_(v, 0, 100)}) {
    node_b->set_pruned_column_ids({});
    node_c->set_pruned_column_ids({});
    // clang-format off
    const auto lqp =
    ProjectionNode::make(expression_vector(y),
      JoinNode::make(JoinMode::Inner, equals_(x, u),
        node_c,
        PredicateNode::make(predicate,
          node_b)));
    // clang-format on

    const auto annotated_lqp = apply_rule(std::make_shared<ColumnPruningRule>(), lqp);
    const auto actual_lqp = apply_rule(rule, annotated_lqp);
    SCOPED_TRACE("For predicate " + predicate->description());
    EXPECT_LQP_EQ(actual_lqp, lqp->deep_copy());
  }
}

TEST_F(JoinToPredicateRewriteRuleTest, NoUnusedJoinSide) {
  // Do not rewrite if columns from b are required upwards in LQP.
  const auto key_constraint_u = TableKeyConstraint{{u->original_column_id}, KeyConstraintType::UNIQUE};
  const auto key_constraint_v = TableKeyConstraint{{v->original_column_id}, KeyConstraintType::UNIQUE};
  node_b->set_key_constraints({key_constraint_u, key_constraint_v});
  const auto order_constraint = TableOrderConstraint{{ColumnID{0}}, {ColumnID{1}}};
  node_b->set_order_constraints({order_constraint});
  const auto foreign_key_constraint =
      ForeignKeyConstraint{{u->original_column_id}, {x->original_column_id}, nullptr, table_c};
  node_b->set_foreign_key_constraints({foreign_key_constraint});

  // clang-format off
  const auto lqp =
  ProjectionNode::make(expression_vector(b, u),
    JoinNode::make(JoinMode::Inner, equals_(a, u),
      node_a,
      PredicateNode::make(equals_(v, 0), node_b)));
  // clang-format on

  const auto annotated_lqp = apply_rule(std::make_shared<ColumnPruningRule>(), lqp);
  const auto actual_lqp = apply_rule(rule, annotated_lqp);
  EXPECT_LQP_EQ(actual_lqp, lqp->deep_copy());
}

TEST_F(JoinToPredicateRewriteRuleTest, Union) {
  // Do not rewrite if there is a union on table b.
  auto key_constraints = TableKeyConstraints{};
  key_constraints.emplace(TableKeyConstraint({u->original_column_id}, KeyConstraintType::UNIQUE));
  key_constraints.emplace(TableKeyConstraint({v->original_column_id}, KeyConstraintType::UNIQUE));
  node_b->set_key_constraints(key_constraints);

  // clang-format off
  const auto lqp =
  ProjectionNode::make(expression_vector(b),
    JoinNode::make(JoinMode::Inner, equals_(a, u),
      node_a,
      PredicateNode::make(equals_(v, 0),
      UnionNode::make(SetOperationMode::Positions, node_b, node_b))));
  // clang-format on

  const auto annotated_lqp = apply_rule(std::make_shared<ColumnPruningRule>(), lqp);
  const auto actual_lqp = apply_rule(rule, annotated_lqp);
  EXPECT_LQP_EQ(actual_lqp, lqp->deep_copy());
}

}  // namespace hyrise
