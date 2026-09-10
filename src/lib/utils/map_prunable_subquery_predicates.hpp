#pragma once

#include <memory>
#include <vector>

#include "expression/abstract_predicate_expression.hpp"
#include "expression/between_expression.hpp"
#include "expression/binary_predicate_expression.hpp"
#include "expression/expression_functional.hpp"
#include "expression/expression_utils.hpp"
#include "expression/lqp_column_expression.hpp"
#include "expression/lqp_subquery_expression.hpp"
#include "expression/pqp_subquery_expression.hpp"
#include "logical_query_plan/stored_table_node.hpp"
#include "operators/get_table.hpp"
#include "types.hpp"

namespace hyrise {

using namespace expression_functional;  // NOLINT(build/namespaces)

/**
 * We use predicates with uncorrelated subqueries for dynamic pruning (see get_table.hpp). These predicates are attached
 * to StoredTableNodes and GetTable operators. We must correctly maintain this information for LQP/PQP deep copies and
 * when translating LQPs to PQPs. We cannot do this while copying/translating the LQP/PQP. Though we keep a mapping from
 * source to target item (LQP node to LQP node for LQP copy, operator to operator for PQP copy, LQP node to operator for
 * translation), we cannot use it for prunable subquery predicates.
 * Our copy/translation happens recursively from root to leaf (StoredTableNode/GetTable). Thus, the mapping does not yet
 * contain the copied/translated predicate that we need in the leaves. As a consequence, we assign prunable subquery
 * predicates after we copied/translated the entire query plan.
 */
template <typename Mapping>
void map_prunable_subquery_predicates(Mapping& mapping) {
  using SourceType = typename Mapping::key_type;
  using TargetType = typename Mapping::mapped_type::element_type;
  using SourceItemType =
      std::conditional_t<std::is_same_v<SourceType, const AbstractOperator*>, GetTable, StoredTableNode>;
  using TargetItemType = std::conditional_t<std::is_same_v<TargetType, AbstractOperator>, GetTable, StoredTableNode>;

  for (const auto& [item, mapped_item] : mapping) {
    // Skip if wrong operator/LQP node.
    if constexpr (std::is_same_v<SourceType, const AbstractOperator*>) {
      if (item->type() != OperatorType::GetTable) {
        continue;
      }
    } else {
      if (item->type != LQPNodeType::StoredTable) {
        continue;
      }
    }

    // Fetch prunable subquery predicates.
    const auto& prunable_subquery_predicates = static_cast<const SourceItemType&>(*item).prunable_subquery_predicates();

    // Skip if the predicates are empty.
    if (prunable_subquery_predicates.empty()) {
      continue;
    }

    // Find mapped predicate for each prunable subquery predicate.
    auto mapped_prunable_subquery_predicates = std::vector<std::shared_ptr<AbstractExpression>>{};
    mapped_prunable_subquery_predicates.reserve(prunable_subquery_predicates.size());
    for (const auto& predicate : prunable_subquery_predicates) {
      auto adjusted_predicate = std::shared_ptr<AbstractExpression>{};
      if constexpr (std::is_same_v<SourceType, const AbstractOperator*>) {
        // PQP copy: PQPSubqueryExpressions use the mapping internally when provided with `deep_copy()`.
        adjusted_predicate = predicate->deep_copy(mapping);
      } else {
        // In other cases, we want to ensure to use the subquery plan already copied by the original predicate. Thus, we
        // refrain from `deep_copy`ing the [PQP|LQP]SubqueryExpressions in the predicate arguments because they would
        // create a new plan copy.
        const auto argument_count = predicate->arguments.size();
        auto adjusted_arguments = std::vector<std::shared_ptr<AbstractExpression>>(argument_count);
        for (auto argument_id = size_t{0}; argument_id < argument_count; ++argument_id) {
          const auto& argument = predicate->arguments[argument_id];
          Assert(argument->type == ExpressionType::LQPSubquery || argument->type == ExpressionType::LQPColumn,
                 "Unexpected subquery predicate with argument '" + argument->as_column_name() + "'.");

          if (argument->type == ExpressionType::LQPColumn) {
            const auto& column_expression = static_cast<const LQPColumnExpression&>(*argument);
            DebugAssert(column_expression.original_node.lock() == item, "Subquery predicate set for wrong node.");

            if constexpr (std::is_same_v<TargetType, AbstractOperator>) {
              // LQP to PQP translation: Create a PQPColumnExpression that points to the original ColumnID.
              adjusted_arguments[argument_id] =
                  pqp_column_(column_expression.original_column_id, column_expression.data_type(),
                              column_expression.as_column_name());
            } else {
              // LQP copy: Copy LQPColumnExpression.
              adjusted_arguments[argument_id] = expression_adapt_to_different_lqp(column_expression, mapping);
            }

            continue;
          }

          const auto& subquery = static_cast<const LQPSubqueryExpression&>(*argument);
          Assert(!subquery.is_correlated(), "Correlated subqueries should not appear in prunable predicates.");
          const auto target_node_it = mapping.find(subquery.lqp);
          Assert(target_node_it != mapping.end(),
                 "Could not find '" + subquery.lqp->description() + "', mapping is invalid.");
          if constexpr (std::is_same_v<TargetType, AbstractOperator>) {
            adjusted_arguments[argument_id] = pqp_subquery_(target_node_it->second, subquery.data_type());
          } else {
            adjusted_arguments[argument_id] = lqp_subquery_(target_node_it->second);
          }
        }

        Assert(predicate->type == ExpressionType::Predicate, "Subquery predicate expression has wrong type.");
        const auto& predicate_expression = static_cast<const AbstractPredicateExpression&>(*predicate);
        const auto predicate_condition = predicate_expression.predicate_condition;
        const auto is_binary_predicate = is_binary_numeric_predicate_condition(predicate_condition);
        Assert(is_binary_predicate || is_between_predicate_condition(predicate_condition),
               "Unsupported predicate condition.");
        Assert(argument_count == (is_binary_predicate ? 2 : 3), "Wrong number of arguments for predicate.");
        if (is_binary_predicate) {
          adjusted_predicate = std::make_shared<BinaryPredicateExpression>(predicate_condition, adjusted_arguments[0],
                                                                           adjusted_arguments[1]);

        } else {
          adjusted_predicate = std::make_shared<BetweenExpression>(predicate_condition, adjusted_arguments[0],
                                                                   adjusted_arguments[1], adjusted_arguments[2]);
        }
      }
      mapped_prunable_subquery_predicates.emplace_back(adjusted_predicate);
    }

    // Set mapped prunable subqery predicates for mapped item.
    static_cast<TargetItemType&>(*mapped_item).set_prunable_subquery_predicates(mapped_prunable_subquery_predicates);
  }
}

}  // namespace hyrise
