#include "strategy_base_test.hpp"

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
#include "logical_query_plan/window_node.hpp"
#include "optimizer/strategy/column_pruning_rule.hpp"

namespace hyrise {

using namespace expression_functional;  // NOLINT(build/namespaces)

class ColumnPruningRuleTest : public StrategyBaseTest {
 public:
  void SetUp() override {
    node_abc = MockNode::make(
        MockNode::ColumnDefinitions{{DataType::Int, "a"}, {DataType::Int, "b"}, {DataType::Int, "c"}}, "a");
    node_uvw = MockNode::make(
        MockNode::ColumnDefinitions{{DataType::Int, "u"}, {DataType::Int, "v"}, {DataType::Int, "w"}}, "b");

    a = node_abc->get_column("a");
    b = node_abc->get_column("b");
    c = node_abc->get_column("c");
    u = node_uvw->get_column("u");
    v = node_uvw->get_column("v");
    w = node_uvw->get_column("w");

    rule = std::make_shared<ColumnPruningRule>();
  }

  const std::shared_ptr<MockNode> pruned(const std::shared_ptr<MockNode> node,
                                         const std::vector<ColumnID>& column_ids) {
    const auto pruned_node = std::static_pointer_cast<MockNode>(node->deep_copy());
    pruned_node->set_pruned_column_ids(column_ids);
    return pruned_node;
  }

  std::shared_ptr<ColumnPruningRule> rule;
  std::shared_ptr<MockNode> node_abc;
  std::shared_ptr<MockNode> node_uvw;
  std::shared_ptr<LQPColumnExpression> a, b, c, u, v, w;
};

TEST_F(ColumnPruningRuleTest, NoUnion) {
  auto lqp = std::shared_ptr<AbstractLQPNode>{};

  // clang-format off
  lqp =
  ProjectionNode::make(expression_vector(add_(mul_(a, u), 5)),
    PredicateNode::make(greater_than_(5, c),
      JoinNode::make(JoinMode::Inner, greater_than_(v, a),
        node_abc,
        SortNode::make(expression_vector(w), std::vector<SortMode>{SortMode::Ascending},  // NOLINT
          node_uvw))));

  // Create deep copy so we can set pruned ColumnIDs on node_abc below without manipulating the input LQP.
  lqp = lqp->deep_copy();

  const auto pruned_node_abc = pruned(node_abc, {ColumnID{1}});
  const auto pruned_a = pruned_node_abc->get_column("a");
  const auto pruned_c = pruned_node_abc->get_column("c");

  const auto actual_lqp = apply_rule(rule, lqp);

  const auto expected_lqp =
  ProjectionNode::make(expression_vector(add_(mul_(pruned_a, u), 5)),
    PredicateNode::make(greater_than_(5, pruned_c),
      JoinNode::make(JoinMode::Inner, greater_than_(v, pruned_a),
        pruned_node_abc,
        SortNode::make(expression_vector(w), std::vector<SortMode>{SortMode::Ascending},  // NOLINT
          node_uvw))));
  // clang-format on

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(ColumnPruningRuleTest, WithUnion) {
  for (auto union_mode : {SetOperationMode::Positions, SetOperationMode::All}) {
    SCOPED_TRACE(std::string{"union_mode: "} + std::string{magic_enum::enum_name(union_mode)});

    auto lqp = std::shared_ptr<AbstractLQPNode>{};

    // clang-format off
    lqp =
    ProjectionNode::make(expression_vector(a),
      UnionNode::make(union_mode,
        PredicateNode::make(greater_than_(a, 5),
          node_abc),
        PredicateNode::make(greater_than_(b, 5),
          node_abc)));

    // Create deep copy so we can set pruned ColumnIDs on node_abc below without manipulating the input LQP.
    lqp = lqp->deep_copy();


    const auto pruned_node_abc = pruned(node_abc, {ColumnID{2}});
    const auto pruned_a = pruned_node_abc->get_column("a");
    const auto pruned_b = pruned_node_abc->get_column("b");

    const auto actual_lqp = apply_rule(rule, lqp);

    // Column c is not used anywhere above the union, so it can be pruned at least in the Positions mode.
    const auto expected_lqp =
    ProjectionNode::make(expression_vector(pruned_a),
      UnionNode::make(union_mode,
        PredicateNode::make(greater_than_(pruned_a, 5),
          pruned_node_abc),
        PredicateNode::make(greater_than_(pruned_b, 5),
          pruned_node_abc)));
    // clang-format on

    EXPECT_LQP_EQ(actual_lqp, expected_lqp);
  }
}

TEST_F(ColumnPruningRuleTest, WithMultipleProjections) {
  auto lqp = std::shared_ptr<AbstractLQPNode>{};

  // clang-format off
  lqp =
  ProjectionNode::make(expression_vector(a),
    PredicateNode::make(greater_than_(mul_(a, b), 5),
      ProjectionNode::make(expression_vector(a, b, mul_(a, b), c),
        PredicateNode::make(greater_than_(mul_(a, 2), 5),
          ProjectionNode::make(expression_vector(a, b, mul_(a, 2), c),
            node_abc)))));

  // Create deep copy so we can set pruned ColumnIDs on node_abc below without manipulating the input LQP.
  lqp = lqp->deep_copy();

  const auto pruned_node_abc = pruned(node_abc, {ColumnID{2}});
  const auto pruned_a = pruned_node_abc->get_column("a");
  const auto pruned_b = pruned_node_abc->get_column("b");

  const auto actual_lqp = apply_rule(rule, lqp);

  const auto expected_lqp =
  ProjectionNode::make(expression_vector(pruned_a),
    PredicateNode::make(greater_than_(mul_(pruned_a, pruned_b), 5),
      ProjectionNode::make(expression_vector(pruned_a, mul_(pruned_a, pruned_b)),
        PredicateNode::make(greater_than_(mul_(pruned_a, 2), 5),
          ProjectionNode::make(expression_vector(pruned_a, pruned_b, mul_(pruned_a, 2)),
           pruned_node_abc)))));
  // clang-format on

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(ColumnPruningRuleTest, ProjectionDoesNotRecompute) {
  auto lqp = std::shared_ptr<AbstractLQPNode>{};

  // clang-format off
  lqp =
  ProjectionNode::make(expression_vector(add_(add_(a, 2), 1)),
    PredicateNode::make(greater_than_(add_(a, 2), 5),
      ProjectionNode::make(expression_vector(add_(a, 2)),
        node_abc)));

  // Create deep copy so we can set pruned ColumnIDs on node_abc below without manipulating the input LQP.
  lqp = lqp->deep_copy();

  const auto pruned_node_abc = pruned(node_abc, {ColumnID{1}, ColumnID{2}});
  const auto pruned_a = pruned_node_abc->get_column("a");

  const auto actual_lqp = apply_rule(rule, lqp);

  const auto expected_lqp =
  ProjectionNode::make(expression_vector(add_(add_(pruned_a, 2), 1)),
    PredicateNode::make(greater_than_(add_(pruned_a, 2), 5),
      ProjectionNode::make(expression_vector(add_(pruned_a, 2)),
        pruned_node_abc)));
  // clang-format on

  // We can be sure that the top projection node does not recompute a+2 because a is not available.

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(ColumnPruningRuleTest, Diamond) {
  auto lqp = std::shared_ptr<AbstractLQPNode>{};

  // clang-format off
  const auto sub_lqp =
  ProjectionNode::make(expression_vector(add_(a, 2), add_(b, 3), add_(c, 4)),
    node_abc);

  lqp =
  ProjectionNode::make(expression_vector(add_(a, 2), add_(b, 3)),
    UnionNode::make(SetOperationMode::Positions,
      PredicateNode::make(greater_than_(add_(a, 2), 5),
        sub_lqp),
      PredicateNode::make(less_than_(add_(b, 3), 10),
        sub_lqp)));

  // Create deep copy so we can set pruned ColumnIDs on node_abc below without manipulating the input LQP.
  lqp = lqp->deep_copy();

  // Column c should be removed even below the UnionNode
  const auto pruned_node_abc = pruned(node_abc, {ColumnID{2}});
  const auto pruned_a = pruned_node_abc->get_column("a");
  const auto pruned_b = pruned_node_abc->get_column("b");

  const auto expected_sub_lqp =
  ProjectionNode::make(expression_vector(add_(pruned_a, 2), add_(pruned_b, 3)),
    pruned_node_abc);

  const auto actual_lqp = apply_rule(rule, lqp);

  const auto expected_lqp =
  ProjectionNode::make(expression_vector(add_(pruned_a, 2), add_(pruned_b, 3)),
    UnionNode::make(SetOperationMode::Positions,
      PredicateNode::make(greater_than_(add_(pruned_a, 2), 5),
        expected_sub_lqp),
      PredicateNode::make(less_than_(add_(pruned_b, 3), 10),
        expected_sub_lqp)));
  // clang-format on

  // We can be sure that the top projection node does not recompute a+2 because a is not available.

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(ColumnPruningRuleTest, SimpleAggregate) {
  auto lqp = std::shared_ptr<AbstractLQPNode>{};

  // clang-format off
  lqp =
  AggregateNode::make(expression_vector(), expression_vector(sum_(add_(a, 2))),
    ProjectionNode::make(expression_vector(a, b, add_(a, 2)),
      node_abc));

  // Create deep copy so we can set pruned ColumnIDs on node_abc below without manipulating the input LQP.
  lqp = lqp->deep_copy();

  const auto pruned_node_abc = pruned(node_abc, {ColumnID{1}, ColumnID{2}});
  const auto pruned_a = pruned_node_abc->get_column("a");

  const auto actual_lqp = apply_rule(rule, lqp);

  const auto expected_lqp =
  AggregateNode::make(expression_vector(), expression_vector(sum_(add_(pruned_a, 2))),
    ProjectionNode::make(expression_vector(add_(pruned_a, 2)),
      pruned_node_abc));
  // clang-format on

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(ColumnPruningRuleTest, UngroupedCountStar) {
  auto lqp = std::shared_ptr<AbstractLQPNode>{};

  // clang-format off
  lqp =
  AggregateNode::make(expression_vector(), expression_vector(count_star_(node_abc)),
    ProjectionNode::make(expression_vector(a, b, add_(a, 2)),
      node_abc));

  // Create deep copy so we can set pruned ColumnIDs on node_abc below without manipulating the input LQP.
  lqp = lqp->deep_copy();

  const auto pruned_node_abc = pruned(node_abc, {ColumnID{1}, ColumnID{2}});
  const auto pruned_a = pruned_node_abc->get_column("a");

  const auto actual_lqp = apply_rule(rule, lqp);

  const auto expected_lqp =
  AggregateNode::make(expression_vector(), expression_vector(count_star_(pruned_node_abc)),
    ProjectionNode::make(expression_vector(pruned_a),
      pruned_node_abc));
  // clang-format on

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(ColumnPruningRuleTest, UngroupedCountStarAndSum) {
  auto lqp = std::shared_ptr<AbstractLQPNode>{};

  // clang-format off
  lqp =
  AggregateNode::make(expression_vector(), expression_vector(count_star_(node_abc), sum_(b)),
    ProjectionNode::make(expression_vector(a, b, add_(a, 2)),
      node_abc));

  // Create deep copy so we can set pruned ColumnIDs on node_abc below without manipulating the input LQP.
  lqp = lqp->deep_copy();

  const auto pruned_node_abc = pruned(node_abc, {ColumnID{0}, ColumnID{2}});
  const auto pruned_b = pruned_node_abc->get_column("b");

  const auto actual_lqp = apply_rule(rule, lqp);

  const auto expected_lqp =
  AggregateNode::make(expression_vector(), expression_vector(count_star_(pruned_node_abc), sum_(pruned_b)),
    ProjectionNode::make(expression_vector(pruned_b),
      pruned_node_abc));
  // clang-format on

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(ColumnPruningRuleTest, GroupedCountStar) {
  auto lqp = std::shared_ptr<AbstractLQPNode>{};

  // clang-format off
  lqp =
  AggregateNode::make(expression_vector(b, a), expression_vector(count_star_(node_abc)),
    ProjectionNode::make(expression_vector(a, b, add_(a, 2)),
      node_abc));

  // Create deep copy so we can set pruned ColumnIDs on node_abc below without manipulating the input LQP.
  lqp = lqp->deep_copy();

  const auto pruned_node_abc = pruned(node_abc, {ColumnID{2}});
  const auto pruned_a = pruned_node_abc->get_column("a");
  const auto pruned_b = pruned_node_abc->get_column("b");

  const auto actual_lqp = apply_rule(rule, lqp);

  const auto expected_lqp =
  AggregateNode::make(expression_vector(pruned_b, pruned_a), expression_vector(count_star_(pruned_node_abc)),
    ProjectionNode::make(expression_vector(pruned_a, pruned_b),
      pruned_node_abc));
  // clang-format on

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(ColumnPruningRuleTest, DoNotPruneUpdateInputs) {
  // Do not prune away input columns to Update, Update needs them all.

  // clang-format off
  const auto select_rows_lqp =
  PredicateNode::make(greater_than_(a, 5),
    node_abc);

  const auto lqp =
  UpdateNode::make("dummy",
    select_rows_lqp,
    ProjectionNode::make(expression_vector(a, add_(b, 1), c),
      select_rows_lqp));
  // clang-format on

  const auto actual_lqp = apply_rule(rule, lqp);
  const auto expected_lqp = lqp->deep_copy();
  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(ColumnPruningRuleTest, DoNotPruneInsertInputs) {
  // Do not prune away input columns to Insert, Insert needs them all.

  // clang-format off
  const auto lqp =
  InsertNode::make("dummy",
    PredicateNode::make(greater_than_(a, 5),
      node_abc));
  // clang-format on

  const auto actual_lqp = apply_rule(rule, lqp);
  const auto expected_lqp = lqp->deep_copy();
  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(ColumnPruningRuleTest, DoNotPruneDeleteInputs) {
  // Do not prune away input columns to Delete, Delete needs them all.

  // clang-format off
  const auto lqp =
  DeleteNode::make(
    PredicateNode::make(greater_than_(a, 5),
      node_abc));
  // clang-format on

  const auto actual_lqp = apply_rule(rule, lqp);
  const auto expected_lqp = lqp->deep_copy();
  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(ColumnPruningRuleTest, DoNotPruneExportInputs) {
  // Do not prune away input columns to Export, Export needs them all.

  // clang-format off
  const auto lqp =
  ExportNode::make("dummy.csv", FileType::Auto,
    PredicateNode::make(greater_than_(a, 5),
      node_abc));
  // clang-format on

  const auto actual_lqp = apply_rule(rule, lqp);
  const auto expected_lqp = lqp->deep_copy();
  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(ColumnPruningRuleTest, DoNotPruneChangeMetaTableInputs) {
  // Do not prune away input columns to ChangeMetaTable, ChangeMetaTable needs them all.

  // clang-format off
  const auto select_rows_lqp =
  PredicateNode::make(greater_than_(a, 5),
    node_abc);

  const auto lqp =
  ChangeMetaTableNode::make("dummy", MetaTableChangeType::Update,
    select_rows_lqp,
    ProjectionNode::make(expression_vector(a, add_(b, 1), c),
      select_rows_lqp));
  // clang-format on

  const auto actual_lqp = apply_rule(rule, lqp);
  const auto expected_lqp = lqp->deep_copy();
  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(ColumnPruningRuleTest, DoNotPruneWindowNodeInputs) {
  // Do not prune away the window function argument, the PARTITION BY columns, and the ORDER BY columns.
  auto frame_description = FrameDescription{FrameType::Range, FrameBound{0, FrameBoundType::Preceding, true},
                                            FrameBound{0, FrameBoundType::CurrentRow, false}};
  const auto window = window_(expression_vector(a), expression_vector(b), std::vector<SortMode>{SortMode::Ascending},
                              std::move(frame_description));

  // clang-format off
  const auto lqp =
  ProjectionNode::make(expression_vector(1),
    WindowNode::make(min_(c, window), node_abc));
  // clang-format on
  const auto expected_lqp = lqp->deep_copy();
  apply_rule(rule, lqp);
  EXPECT_LQP_EQ(lqp, expected_lqp);
}

TEST_F(ColumnPruningRuleTest, PruneInputsNotNeededByWindowNode) {
  // Do not prune away the window the PARTITION BY columns and the ORDER BY columns, but prune additional columns.
  auto frame_description = FrameDescription{FrameType::Range, FrameBound{0, FrameBoundType::Preceding, true},
                                            FrameBound{0, FrameBoundType::CurrentRow, false}};
  const auto window = window_(expression_vector(a), expression_vector(b), std::vector<SortMode>{SortMode::Ascending},
                              std::move(frame_description));

  // clang-format off
  const auto lqp =
  ProjectionNode::make(expression_vector(1),
    WindowNode::make(rank_(window), node_abc));
  // clang-format on

  apply_rule(rule, lqp);
  EXPECT_EQ(node_abc->pruned_column_ids(), std::vector<ColumnID>{ColumnID{2}});
}

TEST_F(ColumnPruningRuleTest, AnnotatePrunableJoinInput) {
  // Join inputs where no expressions are used later in the query plan should be marked as prunable to enable further
  // optimization, such as Join to Semi-Join rewrite. We skip Semi- and Anti-Joins since their right input is always
  // prunable.
  for (const auto join_mode :
       {JoinMode::Inner, JoinMode::Left, JoinMode::Right, JoinMode::FullOuter, JoinMode::Cross}) {
    for (const auto prunable_input_side : {LQPInputSide::Left, LQPInputSide::Right}) {
      const auto join_node =
          join_mode == JoinMode::Cross ? JoinNode::make(join_mode) : JoinNode::make(join_mode, equals_(a, u));
      join_node->set_left_input(node_abc);
      join_node->set_right_input(node_uvw);
      node_abc->set_pruned_column_ids({});
      node_uvw->set_pruned_column_ids({});

      // Project columns of prunable input away.
      const auto projections =
          prunable_input_side == LQPInputSide::Left ? expression_vector(u, v) : expression_vector(a, b);
      const auto lqp = ProjectionNode::make(projections, join_node);

      apply_rule(rule, lqp);

      SCOPED_TRACE("With JoinMode::" + std::string{magic_enum::enum_name(join_mode)});
      EXPECT_TRUE(join_node->prunable_input_side());
      EXPECT_EQ(*join_node->prunable_input_side(), prunable_input_side);
    }
  }
}

}  // namespace hyrise
