#include <memory>
#pragma once

#include <queue>
#include <unordered_set>

#include "operators/abstract_operator.hpp"

namespace hyrise {

enum class PQPVisitation { VisitInputs, DoNotVisitInputs };

/**
 * Calls the passed @param visitor on @param pqp and recursively on its INPUTS. The visitor returns `PQPVisitation`,
 * indicating whether the current operator's input should be visited as well. The algorithm is breadth-first search.
 * Each operator is visited exactly once.
 *
 * @tparam Visitor      Functor called with every operator as a param. Returns `PQPVisitation`.
 */
template <typename Operator, typename Visitor>
void visit_pqp(const std::shared_ptr<Operator>& pqp, Visitor visitor) {
  using AbstractOperatorType = std::conditional_t<std::is_const_v<Operator>, const AbstractOperator, AbstractOperator>;

  auto operator_queue = std::queue<std::shared_ptr<AbstractOperatorType>>{};
  operator_queue.push(pqp);

  auto visited_operators = std::unordered_set<std::shared_ptr<AbstractOperatorType>>{};

  while (!operator_queue.empty()) {
    const auto op = operator_queue.front();
    operator_queue.pop();

    if (!visited_operators.emplace(op).second) {
      continue;
    }

    if (visitor(op) == PQPVisitation::VisitInputs) {
      if constexpr (std::is_const_v<AbstractOperatorType>) {
        if (op->left_input()) {
          operator_queue.push(op->left_input());
        }

        if (op->right_input()) {
          operator_queue.push(op->right_input());
        }
      } else {
        if (op->left_input()) {
          operator_queue.push(op->mutable_left_input());
        }

        if (op->right_input()) {
          operator_queue.push(op->mutable_right_input());
        }
      }
    }
  }
}

/**
 * Gets the value provided by an uncorrelated subquery. Ensures that the subquery was executed and does not return too
 * many values. If the subquery provides an empty result, return NULL_VALUE. Since this function is used by the
 * TableScan to resolve uncorrelated subqueries that can stem from a join rewrite, returning NULL_VALUE for empty
 * results leads to no matching tuples for the scan predicate, which is the same as a join with an empty relation.
 */
AllTypeVariant resolve_uncorrelated_subquery(const std::shared_ptr<const AbstractOperator>& subquery_operator);

}  // namespace hyrise
