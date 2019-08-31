#include "predicate_placement_rule.hpp"
#include "all_parameter_variant.hpp"
#include "expression/expression_utils.hpp"
#include "expression/logical_expression.hpp"
#include "expression/lqp_subquery_expression.hpp"
#include "logical_query_plan/abstract_lqp_node.hpp"
#include "logical_query_plan/join_node.hpp"
#include "logical_query_plan/logical_plan_root_node.hpp"
#include "logical_query_plan/lqp_utils.hpp"
#include "logical_query_plan/predicate_node.hpp"
#include "logical_query_plan/projection_node.hpp"
#include "logical_query_plan/sort_node.hpp"
#include "operators/operator_scan_predicate.hpp"

namespace opossum {

void PredicatePlacementRule::apply_to(const std::shared_ptr<AbstractLQPNode>& node) const {
  // The traversal functions require the existence of a root of the LQP, so make sure we have that
  const auto root_node = node->type == LQPNodeType::Root ? node : LogicalPlanRootNode::make(node);

  std::vector<std::shared_ptr<PredicateNode>> push_down_nodes;
  _push_down_traversal(root_node, LQPInputSide::Left, push_down_nodes);

  _pull_up_traversal(root_node, LQPInputSide::Left);
}

void PredicatePlacementRule::_push_down_traversal(const std::shared_ptr<AbstractLQPNode>& current_node,
                                                  const LQPInputSide input_side,
                                                  std::vector<std::shared_ptr<PredicateNode>>& push_down_nodes) {
  const auto input_node = current_node->input(input_side);
  if (!input_node) return;  // Allow calling without checks

  // A helper method for cases where the input_node does not allow us to proceed
  const auto handle_barrier = [&]() {
    _insert_nodes(current_node, input_side, push_down_nodes);

    if (input_node->left_input()) {
      auto left_push_down_nodes = std::vector<std::shared_ptr<PredicateNode>>{};
      _push_down_traversal(input_node, LQPInputSide::Left, left_push_down_nodes);
    }
    if (input_node->right_input()) {
      auto right_push_down_nodes = std::vector<std::shared_ptr<PredicateNode>>{};
      _push_down_traversal(input_node, LQPInputSide::Right, right_push_down_nodes);
    }
  };

  if (input_node->output_count() > 1) {
    // We cannot push predicates past input_node as doing so would also filter the predicates from the "other" side.
    handle_barrier();
    return;
  }

  switch (input_node->type) {
    case LQPNodeType::Predicate: {
      const auto predicate_node = std::static_pointer_cast<PredicateNode>(input_node);

      if (!_is_expensive_predicate(predicate_node->predicate())) {
        push_down_nodes.emplace_back(predicate_node);

        // As predicate_node might be the input to multiple nodes, remember those nodes before we untie predicate_node
        const auto output_relations = predicate_node->output_relations();

        lqp_remove_node(predicate_node);
        _push_down_traversal(current_node, input_side, push_down_nodes);

        // Restore the output relationships
        for (const auto& [output_node, output_side] : output_relations) {
          output_node->set_input(output_side, current_node->input(input_side));
        }
      } else {
        _push_down_traversal(input_node, input_side, push_down_nodes);
      }
    } break;

    case LQPNodeType::Join: {
      const auto join_node = std::static_pointer_cast<JoinNode>(input_node);

      // Left empty for non-push-past joins
      auto left_push_down_nodes = std::vector<std::shared_ptr<PredicateNode>>{};
      auto right_push_down_nodes = std::vector<std::shared_ptr<PredicateNode>>{};

      // It is safe to move predicates down past Inner, Cross, Semi, AntiNullAsTrue and AntiNullAsFalse Joins
      if (join_node->join_mode == JoinMode::Inner || join_node->join_mode == JoinMode::Cross ||
          join_node->join_mode == JoinMode::Semi || join_node->join_mode == JoinMode::AntiNullAsTrue ||
          join_node->join_mode == JoinMode::AntiNullAsFalse) {
        for (const auto& push_down_node : push_down_nodes) {
          const auto move_to_left = expression_evaluable_on_lqp(push_down_node->predicate(), *join_node->left_input());
          const auto move_to_right =
              expression_evaluable_on_lqp(push_down_node->predicate(), *join_node->right_input());

          if (!move_to_left && !move_to_right && join_node->join_mode == JoinMode::Inner) {
            // The current predicate could not be pushed down to either side. If we cannot push it down, we might be
            // able to create additional predicates that perform some pre-selection before the tuples reach the join.
            // An example can be found in TPC-H query 7, with the predicate
            //   (n1.name = 'DE' AND n2.name = 'FR') OR (n1.name = 'FR' AND n2.n_name = 'DE'
            // We cannot push it to either n1 or n2 as the selected values depend on the result of the other table.
            // However, we can create a predicate (n1.name = 'DE' OR n1.name = 'FR') and reduce the number of tuples
            // that reach the joins from all countries to just two. This behavior is also described in the TPC-H
            // Analyzed paper as "CP4.2b: Join-Dependent Expression Filter Pushdown".
            //
            // For now, this is indiscriminate. For predicates that throw out only few rows, it might actually  // TODO TPC-H 13 seems to regress - evaluate
            // hurt the performance as the predicate will be evaluated twice. Once we see such a case, we could
            // estimate the selectivity of the predicate and decide whether it is worth creating a pre-join predicate.
            //
            // Here are the rules that determine whether we can create a pre-join predicate for the tables l or r with
            // predicates that operate on l (l1, l2), r (r1, r2), or are independent of either table (u1, u2). To
            // produce a predicate for a table, it is required that each expression in the disjunction has a predicate 
            // for that table:
            //
            // (l1 AND r1) OR (l2)        -> create predicate (l1 OR l2) on left side, everything on right side might
            //                               qualify, so do not create a predicate there
            // (l1 AND r2) OR (l2 AND r1) -> create (l1 OR l2) on left, (r1 OR r2) on right (example from above)
            // (l1 AND u1) OR (r1 AND u2) -> do nothing
            // You will also find these examples in the tests.
            //
            // For now, this rule deals only with inner joins. It might also work for other join types, but the
            // implications of creating a pre-join predicate on the NULL-producing side need to be carefully thought
            // through once the need arises.
            //
            // NAMING:
            // Input
            // (l1 AND r2) OR (l2 AND r1)
            // ^^^^^^^^^^^    ^^^^^^^^^^^ outer_disjunction holds two (or more) elements from flattening the OR.
            //                            One of these elements is called expression_in_disjunction.
            //  ^^     ^^                 inner_conjunction holds two (or more) elements from flattening the AND.
            //                            One of these elements is called expression_in_conjunction.

            std::vector<std::shared_ptr<AbstractExpression>> left_disjunction{};
            std::vector<std::shared_ptr<AbstractExpression>> right_disjunction{};

            // Tracks whether we had to abort the search for one of the sides as an inner_conjunction was found that
            // did not cover the side.
            auto aborted_left_side = false;
            auto aborted_right_side = false;

            const auto outer_disjunction = flatten_logical_expressions(push_down_node->predicate(), LogicalOperator::Or);
            for (const auto& expression_in_disjunction : outer_disjunction) {
              // For the current expression_in_disjunction, these hold the PredicateExpressions that need to be true
              // on the left/right side
              std::vector<std::shared_ptr<AbstractExpression>> left_conjunction{};
              std::vector<std::shared_ptr<AbstractExpression>> right_conjunction{};

              // Fill left/right_conjunction
              const auto inner_conjunction = flatten_logical_expressions(expression_in_disjunction, LogicalOperator::And);
              for (const auto& expression_in_conjunction : inner_conjunction) {
                const auto evaluable_on_left_side = expression_evaluable_on_lqp(expression_in_conjunction, *join_node->left_input());
                const auto evaluable_on_right_side = expression_evaluable_on_lqp(expression_in_conjunction, *join_node->right_input());

                // We can only work with expressions that are specific to one side.
                if (evaluable_on_left_side && !evaluable_on_right_side && !aborted_left_side) {
                  left_conjunction.emplace_back(expression_in_conjunction);
                }
                if (evaluable_on_right_side && !evaluable_on_left_side && !aborted_right_side) {
                  right_conjunction.emplace_back(expression_in_conjunction);
                }
              }

              if (!left_conjunction.empty()) {
                // If we have found one or more predicates for the left side, connect them using AND and add them to
                // the disjunction that will be pushed to the left side.
                left_disjunction.emplace_back(inflate_logical_expressions(left_conjunction, LogicalOperator::And));
              } else {
                // If, within the current expression_in_disjunction, we have not found a matching predicate for the
                // left side, all tuples for the left side qualify and it makes no sense to create a filter.
                aborted_left_side = true;
                left_disjunction.clear();
              }
              if (!right_conjunction.empty()) {
                right_disjunction.emplace_back(inflate_logical_expressions(right_conjunction, LogicalOperator::And));
              } else {
                aborted_right_side = true;
                right_disjunction.clear();
              }
            }

            if (!left_disjunction.empty()) {
              // Connect all options (e.g., l.name = 'DE' and l.name = 'FR') with a disjunction, create a predicate,
              // and add it to the list of nodes that will be pushed to the left side
              const auto left_expression = inflate_logical_expressions(left_disjunction, LogicalOperator::Or);
              const auto pre_filter_predicate_node = PredicateNode::make(left_expression);
              left_push_down_nodes.emplace_back(pre_filter_predicate_node);
            }

            if (!right_disjunction.empty()) {
              const auto right_expression = inflate_logical_expressions(right_disjunction, LogicalOperator::Or);
              const auto pre_filter_predicate_node = PredicateNode::make(right_expression);
              right_push_down_nodes.emplace_back(pre_filter_predicate_node);
            }

            _insert_nodes(current_node, input_side, {push_down_node});

            // End of the pre-join filter code
          } else if (move_to_left && move_to_right) {
            // Do not push down uncorrelated predicates
            _insert_nodes(current_node, input_side, {push_down_node});
          } else {
            if (move_to_left) left_push_down_nodes.emplace_back(push_down_node);
            if (move_to_right) right_push_down_nodes.emplace_back(push_down_node);
          }
        }

      } else {
        // We do not push past non-inner/cross joins, place all predicates here
        _insert_nodes(current_node, input_side, push_down_nodes);
      }

      _push_down_traversal(input_node, LQPInputSide::Left, left_push_down_nodes);
      _push_down_traversal(input_node, LQPInputSide::Right, right_push_down_nodes);
    } break;

    case LQPNodeType::Alias:
    case LQPNodeType::Sort:
    case LQPNodeType::Projection: {
      // We can push predicates past these nodes without further consideration
      _push_down_traversal(input_node, LQPInputSide::Left, push_down_nodes);
    } break;

    default: {
      // All not explicitly handled node types are barriers and we do not push predicates past them.
      handle_barrier();
    }
  }
}

std::vector<std::shared_ptr<PredicateNode>> PredicatePlacementRule::_pull_up_traversal(
    const std::shared_ptr<AbstractLQPNode>& current_node, const LQPInputSide input_side) {
  if (!current_node) return {};
  const auto input_node = current_node->input(input_side);
  if (!input_node) return {};

  auto candidate_nodes = _pull_up_traversal(current_node->input(input_side), LQPInputSide::Left);
  auto candidate_nodes_tmp = _pull_up_traversal(current_node->input(input_side), LQPInputSide::Right);
  candidate_nodes.insert(candidate_nodes.end(), candidate_nodes_tmp.begin(), candidate_nodes_tmp.end());

  // Expensive PredicateNodes become candidates for a PullUp, but only IFF they have exactly one output connection.
  // If they have more, we cannot move them.
  if (const auto predicate_node = std::dynamic_pointer_cast<PredicateNode>(input_node);
      predicate_node && _is_expensive_predicate(predicate_node->predicate()) && predicate_node->output_count() == 1) {
    candidate_nodes.emplace_back(predicate_node);
    lqp_remove_node(predicate_node);
  }

  if (current_node->output_count() > 1) {
    // No pull up past nodes with more than one output, because if we did, the other outputs would lose the
    // predicate we pulled up
    _insert_nodes(current_node, input_side, candidate_nodes);
    return {};
  }

  switch (current_node->type) {
    case LQPNodeType::Join: {
      const auto join_node = std::static_pointer_cast<JoinNode>(current_node);

      // It is safe to move predicates down past Inner, Cross, Semi, AntiNullAsTrue and AntiNullAsFalse Joins
      if (join_node->join_mode == JoinMode::Inner || join_node->join_mode == JoinMode::Cross ||
          join_node->join_mode == JoinMode::Semi || join_node->join_mode == JoinMode::AntiNullAsTrue ||
          join_node->join_mode == JoinMode::AntiNullAsFalse) {
        return candidate_nodes;
      } else {
        _insert_nodes(current_node, input_side, candidate_nodes);
        return {};
      }
    } break;

    case LQPNodeType::Alias:
    case LQPNodeType::Predicate:
      return candidate_nodes;

    case LQPNodeType::Projection: {
      auto pull_up_nodes = std::vector<std::shared_ptr<PredicateNode>>{};
      auto blocked_nodes = std::vector<std::shared_ptr<PredicateNode>>{};

      for (const auto& candidate_node : candidate_nodes) {
        if (expression_evaluable_on_lqp(candidate_node->predicate(), *current_node)) {
          pull_up_nodes.emplace_back(candidate_node);
        } else {
          blocked_nodes.emplace_back(candidate_node);
        }
      }

      _insert_nodes(current_node, input_side, blocked_nodes);
      return pull_up_nodes;
    } break;

    default:
      // No pull up past all other node types
      _insert_nodes(current_node, input_side, candidate_nodes);
      return {};
  }

  Fail("GCC thinks this is reachable");
}

void PredicatePlacementRule::_insert_nodes(const std::shared_ptr<AbstractLQPNode>& node, const LQPInputSide input_side,
                                           const std::vector<std::shared_ptr<PredicateNode>>& predicate_nodes) {
  // First node gets inserted on the @param input_side, all others on the left side of their output.
  auto current_node = node;
  auto current_input_side = input_side;

  const auto previous_input_node = node->input(input_side);

  for (const auto& predicate_node : predicate_nodes) {
    current_node->set_input(current_input_side, predicate_node);
    current_node = predicate_node;
    current_input_side = LQPInputSide::Left;
  }

  current_node->set_input(current_input_side, previous_input_node);
}

bool PredicatePlacementRule::_is_expensive_predicate(const std::shared_ptr<AbstractExpression>& predicate) {
  /**
   * We (heuristically) consider a predicate to be expensive if it contains a correlated subquery. Otherwise, we
   * consider it to be cheap
   */
  auto predicate_contains_correlated_subquery = false;
  visit_expression(predicate, [&](const auto& sub_expression) {
    if (const auto subquery_expression = std::dynamic_pointer_cast<LQPSubqueryExpression>(sub_expression);
        subquery_expression && !subquery_expression->arguments.empty()) {
      predicate_contains_correlated_subquery = true;
      return ExpressionVisitation::DoNotVisitArguments;
    } else {
      return ExpressionVisitation::VisitArguments;
    }
  });
  return predicate_contains_correlated_subquery;
}

}  // namespace opossum
