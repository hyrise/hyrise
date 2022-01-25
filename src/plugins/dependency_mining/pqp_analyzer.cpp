#include "pqp_analyzer.hpp"

#include "candidate_strategy/dependent_group_by_candidate_rule.hpp"
#include "candidate_strategy/join_elimination_candidate_rule.hpp"
#include "candidate_strategy/join_to_predicate_candidate_rule.hpp"
#include "candidate_strategy/join_to_semi_candidate_rule.hpp"
#include "expression/expression_functional.hpp"
#include "hyrise.hpp"
#include "logical_query_plan/aggregate_node.hpp"
#include "logical_query_plan/union_node.hpp"
#include "operators/pqp_utils.hpp"
#include "utils/timer.hpp"

namespace {

using namespace opossum;                         // NOLINT
using namespace opossum::expression_functional;  // NOLINT

void gather_expressions_not_computed_by_expression_evaluator(
    const std::shared_ptr<AbstractExpression>& expression,
    const std::vector<std::shared_ptr<AbstractExpression>>& input_expressions,
    ExpressionUnorderedSet& required_expressions, const bool top_level = true) {
  // Top-level expressions are those that are (part of) the ExpressionEvaluator's final result. For example, for an
  // ExpressionEvaluator producing (a + b) + c, the entire expression is a top-level expression. It is the consumer's
  // job to mark it as required. (a + b) however, is required by the ExpressionEvaluator and will be added to
  // required_expressions, as it is not a top-level expression.

  // If an expression that is not a top-level expression is already an input, we require it
  if (std::find_if(input_expressions.begin(), input_expressions.end(),
                   [&expression](const auto& other) { return *expression == *other; }) != input_expressions.end()) {
    if (!top_level) required_expressions.emplace(expression);
    return;
  }

  if (expression->type == ExpressionType::Aggregate || expression->type == ExpressionType::LQPColumn) {
    // Aggregates and LQPColumns are not calculated by the ExpressionEvaluator and are thus required to be part of the
    // input.
    required_expressions.emplace(expression);
    return;
  }

  for (const auto& argument : expression->arguments) {
    gather_expressions_not_computed_by_expression_evaluator(argument, input_expressions, required_expressions, false);
  }
}

ExpressionUnorderedSet gather_locally_required_expressions(
    const std::shared_ptr<const AbstractLQPNode>& node,
    const ExpressionUnorderedSet& expressions_required_by_consumers) {
  // Gathers all expressions required by THIS node, i.e., expressions needed by the node to do its job. For example, a
  // PredicateNode `a < 3` requires the LQPColumn a.
  auto locally_required_expressions = ExpressionUnorderedSet{};

  switch (node->type) {
    // For the vast majority of node types, AbstractLQPNode::node_expression holds all expressions required by this
    // node.
    case LQPNodeType::Alias:
    case LQPNodeType::CreatePreparedPlan:
    case LQPNodeType::CreateView:
    case LQPNodeType::DropView:
    case LQPNodeType::DropTable:
    case LQPNodeType::DummyTable:
    case LQPNodeType::Import:
    case LQPNodeType::Limit:
    case LQPNodeType::Root:
    case LQPNodeType::Sort:
    case LQPNodeType::StaticTable:
    case LQPNodeType::StoredTable:
    case LQPNodeType::Validate:
    case LQPNodeType::Mock: {
      for (const auto& expression : node->node_expressions) {
        locally_required_expressions.emplace(expression);
      }
    } break;

    // For aggregate nodes, we need the group by columns and the arguments to the aggregate functions
    case LQPNodeType::Aggregate: {
      const auto& aggregate_node = static_cast<const AggregateNode&>(*node);
      const auto& node_expressions = node->node_expressions;

      for (auto expression_idx = size_t{0}; expression_idx < node_expressions.size(); ++expression_idx) {
        const auto& expression = node_expressions[expression_idx];
        // The AggregateNode's node_expressions contain both the group_by- and the aggregate_expressions in that order,
        // separated by aggregate_expressions_begin_idx.
        if (expression_idx < aggregate_node.aggregate_expressions_begin_idx) {
          // All group_by-expressions are required
          locally_required_expressions.emplace(expression);
        } else {
          // We need the arguments of all aggregate functions
          DebugAssert(expression->type == ExpressionType::Aggregate, "Expected AggregateExpression");
          if (!AggregateExpression::is_count_star(*expression)) {
            locally_required_expressions.emplace(expression->arguments[0]);
          } else {
            /**
             * COUNT(*) is an edge case: The aggregate function contains a pseudo column expression with an
             * INVALID_COLUMN_ID. We cannot require the latter from other nodes. However, in the end, we have to
             * ensure that the AggregateNode requires at least one expression from other nodes.
             * For
             *  a) grouped COUNT(*) aggregates, this is guaranteed by the group-by column(s).
             *  b) ungrouped COUNT(*) aggregates, it may be guaranteed by other aggregate functions. But, if COUNT(*)
             *     is the only type of aggregate function, we simply require the first output expression from the
             *     left input node.
             */
            if (!locally_required_expressions.empty() || expression_idx < node_expressions.size() - 1) continue;
            locally_required_expressions.emplace(node->left_input()->output_expressions().at(0));
          }
        }
      }
    } break;

    // For ProjectionNodes, collect all expressions that
    //   (1) were already computed and are re-used as arguments in this projection
    //   (2) cannot be computed (i.e., Aggregate and LQPColumn inputs)
    // PredicateNodes have the same requirements - if they have their own implementation, they require all columns to
    // be already computed; if they use the ExpressionEvaluator the columns should at least be computable.
    case LQPNodeType::Predicate:
    case LQPNodeType::Projection: {
      for (const auto& expression : node->node_expressions) {
        if (node->type == LQPNodeType::Projection && !expressions_required_by_consumers.contains(expression)) {
          // An expression produced by a ProjectionNode that is not required by anyone upstream is useless. We should
          // not collect the expressions required for calculating that useless expression.
          continue;
        }

        gather_expressions_not_computed_by_expression_evaluator(expression, node->left_input()->output_expressions(),
                                                                locally_required_expressions);
      }
    } break;

    // For Joins, collect the expressions used on the left and right sides of the join expressions
    case LQPNodeType::Join: {
      const auto& join_node = static_cast<const JoinNode&>(*node);
      for (const auto& predicate : join_node.join_predicates()) {
        DebugAssert(predicate->type == ExpressionType::Predicate && predicate->arguments.size() == 2,
                    "Expected binary predicate for join");
        locally_required_expressions.emplace(predicate->arguments[0]);
        locally_required_expressions.emplace(predicate->arguments[1]);
      }
    } break;

    case LQPNodeType::Union: {
      const auto& union_node = static_cast<const UnionNode&>(*node);
      switch (union_node.set_operation_mode) {
        case SetOperationMode::Positions: {
          // UnionNode does not require any expressions itself for the Positions mode. As Positions by definition
          // operates on the same table left and right, we simply require the same input expressions from both sides.
        } break;

        case SetOperationMode::All: {
          // Similarly, if the two input tables are only glued together, the UnionNode itself does not require any
          // expressions. Currently, this mode is used to merge the result of two mutually exclusive or conditions (see
          // PredicateSplitUpRule). Once we have a union operator that merges data from different tables, we have to
          // look into this more deeply.
          Assert(union_node.left_input()->output_expressions() == union_node.right_input()->output_expressions(),
                 "Can only handle SetOperationMode::All if both inputs have the same expressions");
        } break;

        case SetOperationMode::Unique: {
          // This probably needs all expressions, as all of them are used to establish uniqueness
          Fail("SetOperationMode::Unique is not supported yet");
        }
      }
    } break;

    case LQPNodeType::Intersect:
    case LQPNodeType::Except: {
      Fail("Intersect and Except are not supported yet");
      // Not sure what needs to happen here. That partially depends on how intersect and except are finally implemented.
    } break;

    // No pruning of the input columns for these nodes as they need them all.
    case LQPNodeType::CreateTable:
    case LQPNodeType::Delete:
    case LQPNodeType::Insert:
    case LQPNodeType::Export:
    case LQPNodeType::Update:
    case LQPNodeType::ChangeMetaTable: {
      const auto& left_input_expressions = node->left_input()->output_expressions();
      locally_required_expressions.insert(left_input_expressions.begin(), left_input_expressions.end());

      if (node->right_input()) {
        const auto& right_input_expressions = node->right_input()->output_expressions();
        locally_required_expressions.insert(right_input_expressions.begin(), right_input_expressions.end());
      }
    } break;
  }

  return locally_required_expressions;
}

void recursively_gather_required_expressions(
    const std::shared_ptr<const AbstractLQPNode>& node,
    std::unordered_map<std::shared_ptr<const AbstractLQPNode>, ExpressionUnorderedSet>& required_expressions_by_node,
    std::unordered_map<std::shared_ptr<const AbstractLQPNode>, size_t>& outputs_visited_by_node) {
  auto& required_expressions = required_expressions_by_node[node];
  const auto locally_required_expressions = gather_locally_required_expressions(node, required_expressions);
  required_expressions.insert(locally_required_expressions.begin(), locally_required_expressions.end());

  // We only continue with node's inputs once we have visited all paths above node. We check this by counting the
  // number of the node's outputs that have already been visited. Once we reach the output count, we can continue.
  if (node->type != LQPNodeType::Root) ++outputs_visited_by_node[node];
  if (outputs_visited_by_node[node] < node->output_count()) return;

  // Once all nodes that may require columns from this node (i.e., this node's outputs) have been visited, we can
  // recurse into this node's inputs.
  for (const auto& input : {node->left_input(), node->right_input()}) {
    if (!input) continue;

    // Make sure the entry in required_expressions_by_node exists, then insert all expressions that the current node
    // needs
    auto& required_expressions_for_input = required_expressions_by_node[input];
    const auto& expressions_provided_by_node = input->output_expressions();
    for (const auto& required_expression : required_expressions) {
      // Add the columns needed here (and above) if they come from the input node. Reasons why this might NOT be the
      // case are: (1) The expression is calculated in this node (and is thus not available in the input node), or
      // (2) we have two input nodes (i.e., a join) and the expressions comes from the other side.
      for (auto column_id = ColumnID{0}; column_id < expressions_provided_by_node.size(); ++column_id) {
        if (*expressions_provided_by_node[column_id] == *required_expression) {
          required_expressions_for_input.emplace(required_expression);
          break;
        }
      }
    }

    recursively_gather_required_expressions(input, required_expressions_by_node, outputs_visited_by_node);
  }
}

}  // namespace

namespace opossum {

PQPAnalyzer::PQPAnalyzer(const std::shared_ptr<DependencyCandidateQueue>& queue) : _queue(queue) {
  Assert(Hyrise::get().dependency_usage_config, "No DependencyUsageConfig set");
  if (Hyrise::get().dependency_usage_config->enable_groupby_reduction) {
    add_rule(std::make_unique<DependentGroupByCandidateRule>());
  }
  if (Hyrise::get().dependency_usage_config->enable_join_to_semi) {
    add_rule(std::make_unique<JoinToSemiCandidateRule>());
  }
  if (Hyrise::get().dependency_usage_config->enable_join_to_predicate) {
    add_rule(std::make_unique<JoinToPredicateCandidateRule>());
  }
  if (Hyrise::get().dependency_usage_config->enable_join_elimination) {
    add_rule(std::make_unique<JoinEliminationCandidateRule>());
  }
}

void PQPAnalyzer::run() {
  const auto& pqp_cache = Hyrise::get().default_pqp_cache;
  if (!pqp_cache) {
    std::cout << "NO PQPCache. Stopping" << std::endl;
    return;
  }
  const auto cache_snapshot = pqp_cache->snapshot();

  if (cache_snapshot.empty()) {
    std::cout << "PQPCache empty. Stopping" << std::endl;
    return;
  }

  std::cout << "Run PQPAnalyzer" << std::endl;
  Timer timer;

  for (const auto& [_, entry] : cache_snapshot) {
    const auto pqp_root = entry.value;
    const auto& lqp_root = pqp_root->lqp_node;
    if (!lqp_root) {
      std::cout << " No LQP root found" << std::endl;
      continue;
    }

    std::unordered_map<std::shared_ptr<const AbstractLQPNode>, ExpressionUnorderedSet> required_expressions_by_node;
    std::unordered_map<std::shared_ptr<const AbstractLQPNode>, size_t> outputs_visited_by_node;
    if (Hyrise::get().dependency_usage_config->enable_join_to_semi ||
        Hyrise::get().dependency_usage_config->enable_join_to_predicate ||
        Hyrise::get().dependency_usage_config->enable_join_elimination) {
      // Add top-level columns that need to be included as they are the actual output
      const auto output_expressions = lqp_root->output_expressions();
      required_expressions_by_node[lqp_root].insert(output_expressions.cbegin(), output_expressions.cend());
      recursively_gather_required_expressions(lqp_root, required_expressions_by_node, outputs_visited_by_node);
    }

    visit_pqp(pqp_root, [&](const auto& op) {
      const auto& lqp_node = op->lqp_node;
      if (!lqp_node) {
        return PQPVisitation::VisitInputs;
      }
      auto prio = static_cast<size_t>(op->performance_data->walltime.count());

      const auto& current_rules = _rules[lqp_node->type];
      for (const auto& rule : current_rules) {
        auto candidates = rule->apply_to_node(lqp_node, op, prio, required_expressions_by_node);
        for (auto& candidate : candidates) {
          _add_if_new(candidate);
        }
      }
      return PQPVisitation::VisitInputs;
    });
  }

  if (_queue) {
    for (auto& candidate : _known_candidates) {
      _queue->emplace(candidate);
    }
  }

  std::cout << "PQPAnalyzer generated " << _known_candidates.size() << " candidates in " << timer.lap_formatted()
            << std::endl;
}

void PQPAnalyzer::add_rule(std::unique_ptr<AbstractDependencyCandidateRule> rule) {
  _rules[rule->target_node_type].emplace_back(std::move(rule));
}

void PQPAnalyzer::_add_if_new(DependencyCandidate& candidate) {
  auto it = _known_candidates.find(candidate);
  if (it == _known_candidates.end()) {
    _known_candidates.emplace(std::move(candidate));
    return;
  }
  auto& known_candidate = *it;
  if (known_candidate.priority < candidate.priority) {
    known_candidate.priority = candidate.priority;
  }
}

}  // namespace opossum
