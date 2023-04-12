#include "chunk_pruning_rule.hpp"

#include <algorithm>
#include <iostream>

#include "all_parameter_variant.hpp"
#include "expression/expression_utils.hpp"
#include "expression/lqp_subquery_expression.hpp"
#include "logical_query_plan/abstract_lqp_node.hpp"
#include "logical_query_plan/join_node.hpp"
#include "logical_query_plan/lqp_utils.hpp"
#include "logical_query_plan/predicate_node.hpp"
#include "logical_query_plan/stored_table_node.hpp"
#include "utils/assert.hpp"

namespace {

using namespace hyrise;  // NOLINT(build/namespaces)

/**
 * This function traverses the LQP upwards from @param next_node to find all predicates that filter
 * @param stored_table_node. If the LQP branches, recursion is used to continue @param current_predicate_pruning_chain
 * for all branches.
 * @param visited_predicate_nodes is for debugging purposes only.
 *
 * @returns all predicate pruning chain(s) that filter @param stored_table_node. Note, that these chains can overlap
 * in their first few nodes.
 */
std::vector<PredicatePruningChain> find_predicate_pruning_chains_by_stored_table_node_recursively(
    const std::shared_ptr<AbstractLQPNode>& next_node, PredicatePruningChain current_predicate_pruning_chain,
    const std::shared_ptr<StoredTableNode>& stored_table_node,
    std::unordered_set<std::shared_ptr<PredicateNode>>& visited_predicate_nodes) {
  auto predicate_pruning_chains = std::vector<PredicatePruningChain>{};

  visit_lqp_upwards(next_node, [&](const auto& current_node) {
    /**
     * In the following switch-statement, we
     *  (1) add PredicateNodes to the current predicate pruning chain, if applicable, and
     *  (2) check whether the predicate pruning chain continues or ends with current_node.
     */
    auto predicate_pruning_chain_continues = true;
    switch (current_node->type) {
      case LQPNodeType::Alias:
      case LQPNodeType::Sort:
      case LQPNodeType::StoredTable:
      case LQPNodeType::Validate:
      case LQPNodeType::Projection:
        break;
      case LQPNodeType::Predicate: {
        const auto& predicate_node = std::static_pointer_cast<PredicateNode>(current_node);
        /**
         * The recursion of this function should not lead to a repeated visitation of nodes. Due to the assumption that
         * predicate pruning chains do not merge after having branched out, predicate nodes are checked for
         * revisitation.
         */
        Assert(!visited_predicate_nodes.contains(predicate_node),
               "Predicate chains are not expected to merge after having branched.");
        visited_predicate_nodes.insert(predicate_node);

        // PredicateNode might not belong to the current predicate pruning chain, e.g., when it follows a JoinNode and
        // references LQPColumnExpressions from other StoredTableNodes.
        auto belongs_to_predicate_pruning_chain = true;
        const auto& predicate_expression = predicate_node->predicate();
        visit_expression(predicate_expression, [&](const auto& expression) {
          if (expression->type != ExpressionType::LQPColumn) {
            return ExpressionVisitation::VisitArguments;
          }
          const auto& column_expression = std::static_pointer_cast<LQPColumnExpression>(expression);
          if (column_expression->original_node.lock() != stored_table_node) {
            // PredicateNode does not filter stored_table_node.
            // Therefore, it is not added to the current predicate pruning chain.
            belongs_to_predicate_pruning_chain = false;
          }
          return ExpressionVisitation::DoNotVisitArguments;
        });
        if (belongs_to_predicate_pruning_chain) {
          current_predicate_pruning_chain.emplace_back(predicate_node);
        }
      } break;
      case LQPNodeType::Join: {
        // Check whether the predicate pruning chain can continue after the join.
        predicate_pruning_chain_continues = false;
        auto join_node = std::static_pointer_cast<JoinNode>(current_node);
        for (const auto& expression : join_node->output_expressions()) {
          if (expression->type != ExpressionType::LQPColumn) {
            continue;
          }
          const auto column_expression = std::static_pointer_cast<LQPColumnExpression>(expression);
          if (column_expression->original_node.lock() == stored_table_node) {
            // At least one column expression of stored_table_node survives the join.
            predicate_pruning_chain_continues = true;
            break;
          }
        }
      } break;
      default:
        // For all other types of nodes, we cancel the predicate pruning chain.
        predicate_pruning_chain_continues = false;
    }

    if (!predicate_pruning_chain_continues) {
      // We do not have to go the LQP-tree up further because the predicate pruning chain is complete.
      predicate_pruning_chains.emplace_back(current_predicate_pruning_chain);
      return LQPUpwardVisitation::DoNotVisitOutputs;
    }

    /**
     * In case the predicate pruning chain branches, we use recursion to continue the predicate chain for each branch
     * individually.
     */
    if (current_node->outputs().size() > 1) {
      for (auto& output_node : current_node->outputs()) {
        auto continued_predicate_pruning_chains = find_predicate_pruning_chains_by_stored_table_node_recursively(
            output_node, current_predicate_pruning_chain, stored_table_node, visited_predicate_nodes);

        predicate_pruning_chains.insert(predicate_pruning_chains.end(), continued_predicate_pruning_chains.begin(),
                                        continued_predicate_pruning_chains.end());
      }
      return LQPUpwardVisitation::DoNotVisitOutputs;
    }

    // Continue without recursion.
    return LQPUpwardVisitation::VisitOutputs;
  });

  return predicate_pruning_chains;
}

}  // namespace

namespace hyrise {

std::string ChunkPruningRule::name() const {
  static const auto name = std::string{"ChunkPruningRule"};
  return name;
}

void ChunkPruningRule::_apply_to_plan_without_subqueries(const std::shared_ptr<AbstractLQPNode>& lqp_root) const {
  auto predicate_pruning_chains_by_stored_table_node =
      std::unordered_map<std::shared_ptr<StoredTableNode>, std::vector<PredicatePruningChain>>{};

  // (1) Collect all StoredTableNodes and find the chains of PredicateNodes that sit on top of them.
  const auto nodes = lqp_find_nodes_by_type(lqp_root, LQPNodeType::StoredTable);
  for (const auto& node : nodes) {
    const auto& stored_table_node = std::static_pointer_cast<StoredTableNode>(node);
    predicate_pruning_chains_by_stored_table_node.emplace(
        stored_table_node, _find_predicate_pruning_chains_by_stored_table_node(stored_table_node));
  }

  // (2) Set pruned chunks for each StoredTableNode.
  for (const auto& [stored_table_node, predicate_pruning_chains] : predicate_pruning_chains_by_stored_table_node) {
    if (predicate_pruning_chains.empty()) {
      continue;
    }

    // (2.1) Determine set of pruned chunks per predicate pruning chain.
    auto pruned_chunk_id_sets = std::vector<std::set<ChunkID>>{};
    for (const auto& predicate_pruning_chain : predicate_pruning_chains) {
      auto exclusions = compute_chunk_exclude_list(predicate_pruning_chain, stored_table_node,
                                                   _excluded_chunk_ids_by_predicate_node_cache);
      pruned_chunk_id_sets.emplace_back(std::move(exclusions));
    }

    // (2.2) Calculate the intersection of pruned chunks across all predicate pruning chains.
    const auto& pruned_chunk_ids = _intersect_chunk_ids(pruned_chunk_id_sets);
    if (!pruned_chunk_ids.empty()) {
      // (2.3) Set the pruned chunk ids of stored_table_node.
      DebugAssert(stored_table_node->pruned_chunk_ids().empty(),
                  "Did not expect a StoredTableNode with an already existing set of pruned chunk ids.");
      // Wanted side effect of using std::set: pruned_chunk_ids vector is already sorted.
      stored_table_node->set_pruned_chunk_ids(std::vector<ChunkID>(pruned_chunk_ids.begin(), pruned_chunk_ids.end()));
    }

    // (2.4) Get and set predicates with uncorrelated subqueries so we can use them for pruning during execution.
    // (2.4.1) Collect predicates with uncorrelated subqueries that are part of each chain in a new "pseudo" chain. When
    //         we want to use them for pruning during execution, it is safe to add the chunks pruned by them to the
    //         already pruned chunks.
    const auto chain_count = predicate_pruning_chains.size();
    auto chain_count_per_subquery_predicate = std::unordered_map<std::shared_ptr<PredicateNode>, uint64_t>{};
    auto prunable_subquery_predicates = std::vector<std::weak_ptr<AbstractLQPNode>>{};
    for (const auto& predicate_chain : predicate_pruning_chains) {
      for (const auto& predicate_node : predicate_chain) {
        // Only use binary and between predicates that can easily be used for pruning. Do not use, e.g, InExpressions
        // etc., which might end up in the ExpressionEvaluator anyways.
        const auto& predicate = std::dynamic_pointer_cast<AbstractPredicateExpression>(predicate_node->predicate());
        if (!predicate) {
          continue;
        }
        const auto predicate_condition = predicate->predicate_condition;
        if (!is_binary_numeric_predicate_condition(predicate_condition) &&
            !is_between_predicate_condition(predicate_condition)) {
          continue;
        }

        for (auto& argument : predicate->arguments) {
          if (argument->type == ExpressionType::LQPSubquery &&
              !static_cast<const LQPSubqueryExpression&>(*argument).is_correlated()) {
            // Count the number of occurrences and add the predicate when it appears in all chains.
            const auto occurrence_count = ++chain_count_per_subquery_predicate[predicate_node];
            if (occurrence_count == chain_count) {
              prunable_subquery_predicates.emplace_back(predicate_node);
            }
            // Make sure we do not count `x BETWEEN (SELECT MIN(y) ...) AND (SELECT (MAX(y) ...)` twice.
            break;
          }
        }
      }
    }

    // (2.4.2) Set the predicates that might be used for pruning during execution.
    if (!prunable_subquery_predicates.empty()) {
      stored_table_node->set_prunable_subquery_predicates(prunable_subquery_predicates);
    }
  }
}

/**
 * @returns chains of PredicateNodes that sit on top of the given @param stored_table_node.
 */
std::vector<PredicatePruningChain> ChunkPruningRule::_find_predicate_pruning_chains_by_stored_table_node(
    const std::shared_ptr<StoredTableNode>& stored_table_node) {
  /**
   * In the following, we use a recursive function to traverse the LQP upwards from stored_table_node. It returns all
   * predicate pruning chains that filter stored_table_node.
   * It takes the following four arguments:
   *   1. The first argument marks the starting point of the LQP upwards-traversal. Hence, we pass stored_table_node.
   *   2. The second argument refers to the current predicate pruning chain. Since the LQP traversal has not yet
   *      started, we pass an empty vector.
   *   3. The third argument refers to the StoredTableNode for which predicate pruning chains should be returned.
   *      Therefore, we have to pass stored_table_node again.
   *   4. The fourth argument is used for debugging purposes: The following function's recursion should not lead to a
   *      repeated visitation of nodes. Due to the assumption that predicate pruning chains do not merge after
   *      having branched out, visited predicate nodes are tracked and checked for revisitation in an Assert.
   */
  auto visited_predicate_nodes = std::unordered_set<std::shared_ptr<PredicateNode>>{};
  return find_predicate_pruning_chains_by_stored_table_node_recursively(stored_table_node, {}, stored_table_node,
                                                                        visited_predicate_nodes);
}

std::set<ChunkID> ChunkPruningRule::_intersect_chunk_ids(const std::vector<std::set<ChunkID>>& chunk_id_sets) {
  if (chunk_id_sets.empty() || chunk_id_sets.at(0).empty()) {
    return {};
  }
  if (chunk_id_sets.size() == 1) {
    return chunk_id_sets.at(0);
  }

  auto chunk_id_set = chunk_id_sets.at(0);
  for (auto set_idx = size_t{1}; set_idx < chunk_id_sets.size(); ++set_idx) {
    const auto& current_chunk_id_set = chunk_id_sets.at(set_idx);
    if (current_chunk_id_set.empty()) {
      return {};
    }

    auto intersection = std::set<ChunkID>{};
    std::set_intersection(chunk_id_set.begin(), chunk_id_set.end(), current_chunk_id_set.begin(),
                          current_chunk_id_set.end(), std::inserter(intersection, intersection.end()));
    chunk_id_set = std::move(intersection);
  }

  return chunk_id_set;
}

}  // namespace hyrise
