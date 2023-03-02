#include "candidate_strategy_base_test.hpp"

#include "../../../../plugins/dependency_discovery/candidate_strategy/join_to_predicate_candidate_rule.hpp"
#include "expression/expression_functional.hpp"
#include "hyrise.hpp"
#include "logical_query_plan/join_node.hpp"
#include "logical_query_plan/predicate_node.hpp"
#include "logical_query_plan/stored_table_node.hpp"
#include "logical_query_plan/union_node.hpp"
#include "utils/load_table.hpp"

namespace hyrise {

using namespace expression_functional;  // NOLINT(build/namespaces)

class JoinToPredicateCandidateRuleTest : public CandidateStrategyBaseTest,
                                         public ::testing::WithParamInterface<PredicateCondition> {
 protected:
  void SetUp() override {
    _rule = std::make_unique<JoinToPredicateCandidateRule>();
    const auto& table = load_table("resources/test_data/tbl/int_int_int.tbl");
    Hyrise::get().storage_manager.add_table(_table_name_a, table);
    Hyrise::get().storage_manager.add_table(_table_name_b, table);

    _table_a = StoredTableNode::make(_table_name_a);
    _table_b = StoredTableNode::make(_table_name_b);

    _a = _table_a->get_column("a");
    _b = _table_a->get_column("b");
    _c = _table_a->get_column("c");

    _x = _table_b->get_column("a");
    _y = _table_b->get_column("b");
    _z = _table_b->get_column("c");
  }

  void _prune(const std::shared_ptr<JoinNode>& join_node) {
    switch (join_node->join_mode) {
      case JoinMode::Inner:
      case JoinMode::Left:
      case JoinMode::Cross:
      case JoinMode::FullOuter:
        join_node->mark_input_side_as_prunable(LQPInputSide::Right);
        return;
      case JoinMode::Right:
        join_node->mark_input_side_as_prunable(LQPInputSide::Left);
        return;
      case JoinMode::Semi:
      case JoinMode::AntiNullAsFalse:
      case JoinMode::AntiNullAsTrue:
        return;
    }
  }

  std::shared_ptr<AbstractPredicateExpression> _create_predicate(const std::shared_ptr<AbstractExpression>& expression,
                                                                 const PredicateCondition predicate_condition) {
    if (predicate_condition == PredicateCondition::Equals) {
      return equals_(expression, 9);
    }

    return std::make_shared<BetweenExpression>(predicate_condition, expression, value_(9), value_(11));
  }

  std::shared_ptr<StoredTableNode> _table_a, _table_b;
  const std::string _table_name_a{"t_a"};
  const std::string _table_name_b{"t_b"};
  std::shared_ptr<LQPColumnExpression> _a, _b, _c, _x, _y, _z;
};

INSTANTIATE_TEST_SUITE_P(JoinToPredicateCandidateRuleTestInstances, JoinToPredicateCandidateRuleTest,
                         ::testing::Values(PredicateCondition::BetweenInclusive,
                                           PredicateCondition::BetweenLowerExclusive,
                                           PredicateCondition::BetweenUpperExclusive,
                                           PredicateCondition::BetweenExclusive),
                         enum_formatter<PredicateCondition>);

TEST_F(JoinToPredicateCandidateRuleTest, CandidatesEqualsPredicate) {
  for (const auto join_mode : {JoinMode::Inner, JoinMode::Semi}) {
    // clang-format off
    const auto lqp =
    JoinNode::make(join_mode, equals_(_a, _x),
      PredicateNode::make(equals_(_b, 10),
        _table_a),
      PredicateNode::make(equals_(_z, 9),
        _table_b));
    // clang-format on

    _prune(lqp);

    const auto& dependency_candidates = _apply_rule(lqp);
    SCOPED_TRACE("for JoinMode::" + std::string{magic_enum::enum_name(join_mode)});
    const auto expected_candidate_count = join_mode == JoinMode::Inner ? 4 : 3;
    EXPECT_EQ(dependency_candidates.size(), expected_candidate_count);

    const auto ucc_candidate_join_column = std::make_shared<UccCandidate>(_table_name_b, _x->original_column_id);
    const auto ucc_candidate_predicate_column = std::make_shared<UccCandidate>(_table_name_b, _z->original_column_id);
    const auto ind_candidate =
        std::make_shared<IndCandidate>(_table_name_a, _a->original_column_id, _table_name_b, _x->original_column_id);
    const auto od_candidate =
        std::make_shared<OdCandidate>(_table_name_b, _x->original_column_id, _z->original_column_id);

    EXPECT_TRUE(dependency_candidates.contains(ucc_candidate_predicate_column));
    EXPECT_TRUE(dependency_candidates.contains(ind_candidate));
    EXPECT_TRUE(dependency_candidates.contains(od_candidate));

    if (join_mode == JoinMode::Inner) {
      EXPECT_TRUE(dependency_candidates.contains(ucc_candidate_join_column));
    }
  }
}

TEST_F(JoinToPredicateCandidateRuleTest, CandidatesBetweenPredicate) {
  for (const auto join_mode : {JoinMode::Inner, JoinMode::Semi}) {
    // clang-format off
    const auto lqp =
    JoinNode::make(join_mode, equals_(_a, _x),
      PredicateNode::make(equals_(_b, 10),
        _table_a),
      PredicateNode::make(between_inclusive_(_z, 9, 10),
        _table_b));
    // clang-format on

    _prune(lqp);

    const auto& dependency_candidates = _apply_rule(lqp);

    SCOPED_TRACE("for JoinMode::" + std::string{magic_enum::enum_name(join_mode)});
    const auto expected_candidate_count = join_mode == JoinMode::Inner ? 3 : 2;
    EXPECT_EQ(dependency_candidates.size(), expected_candidate_count);

    const auto ucc_candidate_join_column = std::make_shared<UccCandidate>(_table_name_b, _x->original_column_id);
    const auto ind_candidate =
        std::make_shared<IndCandidate>(_table_name_a, _a->original_column_id, _table_name_b, _x->original_column_id);
    const auto od_candidate =
        std::make_shared<OdCandidate>(_table_name_b, _x->original_column_id, _z->original_column_id);

    EXPECT_TRUE(dependency_candidates.contains(ind_candidate));
    EXPECT_TRUE(dependency_candidates.contains(od_candidate));

    if (join_mode == JoinMode::Inner) {
      EXPECT_TRUE(dependency_candidates.contains(ucc_candidate_join_column));
    }
  }
}

TEST_P(JoinToPredicateCandidateRuleTest, CandidatesPredicateRightInputInnerJoin) {
  const auto predicate = _create_predicate(_z, GetParam());
  // clang-format off
    const auto lqp =
    JoinNode::make(JoinMode::Inner, equals_(_a, _x),
      PredicateNode::make(equals_(_b, 10),
        _table_a),
      PredicateNode::make(predicate,
        _table_b));
  // clang-format on

  _prune(lqp);

  const auto& dependency_candidates = _apply_rule(lqp);

  const auto is_equals_predicate = predicate->predicate_condition == PredicateCondition::Equals;
  const auto candidate_count = is_equals_predicate ? 4 : 3;
  EXPECT_EQ(dependency_candidates.size(), candidate_count);

  const auto ucc_candidate_join_column = std::make_shared<UccCandidate>(_table_name_b, _x->original_column_id);
  const auto ind_candidate =
      std::make_shared<IndCandidate>(_table_name_a, _a->original_column_id, _table_name_b, _x->original_column_id);
  const auto od_candidate =
      std::make_shared<OdCandidate>(_table_name_b, _x->original_column_id, _z->original_column_id);

  EXPECT_TRUE(dependency_candidates.contains(ucc_candidate_join_column));
  EXPECT_TRUE(dependency_candidates.contains(ind_candidate));
  EXPECT_TRUE(dependency_candidates.contains(od_candidate));

  if (is_equals_predicate) {
    const auto ucc_candidate_predicate_column = std::make_shared<UccCandidate>(_table_name_b, _z->original_column_id);
    EXPECT_TRUE(dependency_candidates.contains(ucc_candidate_predicate_column));
  }
}

TEST_P(JoinToPredicateCandidateRuleTest, CandidatesPredicateRightInputSemiJoin) {
  const auto predicate = _create_predicate(_z, GetParam());
  // clang-format off
    const auto lqp =
    JoinNode::make(JoinMode::Semi, equals_(_a, _x),
      PredicateNode::make(equals_(_b, 10),
        _table_a),
      PredicateNode::make(predicate,
        _table_b));
  // clang-format on

  _prune(lqp);

  const auto& dependency_candidates = _apply_rule(lqp);

  const auto is_equals_predicate = predicate->predicate_condition == PredicateCondition::Equals;
  const auto candidate_count = is_equals_predicate ? 3 : 2;
  EXPECT_EQ(dependency_candidates.size(), candidate_count);

  const auto ind_candidate =
      std::make_shared<IndCandidate>(_table_name_a, _a->original_column_id, _table_name_b, _x->original_column_id);
  const auto od_candidate =
      std::make_shared<OdCandidate>(_table_name_b, _x->original_column_id, _z->original_column_id);

  EXPECT_TRUE(dependency_candidates.contains(ind_candidate));
  EXPECT_TRUE(dependency_candidates.contains(od_candidate));

  if (is_equals_predicate) {
    const auto ucc_candidate_predicate_column = std::make_shared<UccCandidate>(_table_name_b, _z->original_column_id);
    EXPECT_TRUE(dependency_candidates.contains(ucc_candidate_predicate_column));
  }
}

TEST_P(JoinToPredicateCandidateRuleTest, CandidatesPredicateLeftInput) {
  const auto predicate = _create_predicate(_z, GetParam());
  // clang-format off
    const auto lqp =
    JoinNode::make(JoinMode::Inner, equals_(_a, _x),
      PredicateNode::make(predicate,
        _table_b),
      PredicateNode::make(equals_(_b, 10),
        _table_a));
  // clang-format on

  lqp->mark_input_side_as_prunable(LQPInputSide::Left);
  const auto& dependency_candidates = _apply_rule(lqp);

  const auto is_equals_predicate = predicate->predicate_condition == PredicateCondition::Equals;
  const auto candidate_count = is_equals_predicate ? 4 : 3;
  EXPECT_EQ(dependency_candidates.size(), candidate_count);

  const auto ucc_candidate_join_column = std::make_shared<UccCandidate>(_table_name_b, _x->original_column_id);
  const auto ind_candidate =
      std::make_shared<IndCandidate>(_table_name_a, _a->original_column_id, _table_name_b, _x->original_column_id);
  const auto od_candidate =
      std::make_shared<OdCandidate>(_table_name_b, _x->original_column_id, _z->original_column_id);

  EXPECT_TRUE(dependency_candidates.contains(ucc_candidate_join_column));
  EXPECT_TRUE(dependency_candidates.contains(ind_candidate));
  EXPECT_TRUE(dependency_candidates.contains(od_candidate));

  if (is_equals_predicate) {
    const auto ucc_candidate_predicate_column = std::make_shared<UccCandidate>(_table_name_b, _z->original_column_id);
    EXPECT_TRUE(dependency_candidates.contains(ucc_candidate_predicate_column));
  }
}

TEST_P(JoinToPredicateCandidateRuleTest, NoCandidatesNoPrunableSide) {
  const auto predicate = _create_predicate(_z, GetParam());
  // clang-format off
    const auto lqp =
    JoinNode::make(JoinMode::Inner, equals_(_a, _x),
      PredicateNode::make(predicate,
        _table_b),
      PredicateNode::make(equals_(_b, 10),
        _table_a));
  // clang-format on

  const auto& dependency_candidates = _apply_rule(lqp);
  EXPECT_TRUE(dependency_candidates.empty());
}

TEST_F(JoinToPredicateCandidateRuleTest, NoCandidatesUnsupportedJoinModes) {
  const auto join_modes = {JoinMode::Left,           JoinMode::Right,           JoinMode::FullOuter,
                           JoinMode::AntiNullAsTrue, JoinMode::AntiNullAsFalse, JoinMode::Cross};
  for (const auto join_mode : join_modes) {
    const auto lqp =
        join_mode == JoinMode::Cross ? JoinNode::make(JoinMode::Cross) : JoinNode::make(join_mode, equals_(_a, _x));

    _prune(lqp);

    lqp->set_left_input(PredicateNode::make(equals_(_b, 10), _table_a));
    lqp->set_right_input(PredicateNode::make(equals_(_z, 9), _table_b));

    const auto& dependency_candidates = _apply_rule(lqp);
    EXPECT_TRUE(dependency_candidates.empty()) << "for JoinMode::" << join_mode;
  }
}

TEST_F(JoinToPredicateCandidateRuleTest, NoCandidatesComplexPredicates) {
  const auto join_modes = {JoinMode::Inner, JoinMode::Semi};
  for (const auto join_mode : join_modes) {
    // clang-format off
    const auto lqp =
    JoinNode::make(join_mode, equals_(add_(_a, 1), add_(_x, 1)),
      PredicateNode::make(equals_(_b, 10),
        _table_a),
      PredicateNode::make(equals_(_z, 9),
        _table_b));
    // clang-format on

    _prune(lqp);

    const auto& dependency_candidates = _apply_rule(lqp);
    EXPECT_TRUE(dependency_candidates.empty()) << "for JoinMode::" << join_mode;
  }
}

TEST_F(JoinToPredicateCandidateRuleTest, NoCandidatesSelfJoin) {
  for (const auto& predicate : expression_vector(equals_(_z, 9), between_inclusive_(_z, 9, 10))) {
    for (const auto join_mode : {JoinMode::Inner, JoinMode::Semi}) {
      // clang-format off
    const auto lqp =
    JoinNode::make(join_mode, equals_(_x, _y),
      PredicateNode::make(equals_(_x, 10),
        _table_b),
      PredicateNode::make(predicate,
        _table_b));
      // clang-format on

      _prune(lqp);

      const auto& dependency_candidates = _apply_rule(lqp);
      SCOPED_TRACE("for JoinMode::" + std::string{magic_enum::enum_name(join_mode)} + " and predicate " +
                   predicate->description());
      EXPECT_TRUE(dependency_candidates.empty());
    }
  }
}

}  // namespace hyrise
