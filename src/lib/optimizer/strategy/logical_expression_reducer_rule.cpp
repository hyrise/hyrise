#include "logical_expression_reducer_rule.hpp"

#include <functional>
#include <unordered_set>

#include "expression/expression_functional.hpp"
#include "expression/expression_utils.hpp"
#include "expression/logical_expression.hpp"
#include "logical_query_plan/abstract_lqp_node.hpp"
#include "logical_query_plan/predicate_node.hpp"
#include "logical_query_plan/projection_node.hpp"

namespace opossum {

using namespace opossum::expression_functional;  // NOLINT

std::string LogicalExpressionReducerRule::name() const { return "Logical Expression Reducer Rule"; }

namespace {

void collect_chained_logical_expressions(const std::shared_ptr<AbstractExpression>& expression,
                                         LogicalOperator logical_operator, ExpressionUnorderedSet& result) {
  if (expression->type != ExpressionType::Logical) {
    // Not a logical expression, so for our purposes, we consider it "atomic"
    result.emplace(expression);
    return;
  }

  // Potentially continuing a chain of logical ANDs/ORs
  const auto& logical_expression = std::static_pointer_cast<LogicalExpression>(expression);
  if (logical_expression->logical_operator != logical_operator) {
    // A logical expression, but not of the right type
    result.emplace(expression);
    return;
  }

  collect_chained_logical_expressions(logical_expression->left_operand(), logical_operator, result);
  collect_chained_logical_expressions(logical_expression->right_operand(), logical_operator, result);
}

void remove_expressions_from_chain(std::shared_ptr<AbstractExpression>& chain, LogicalOperator logical_operator,
                                   const ExpressionUnorderedSet& expressions_to_remove) {
  if (chain->type != ExpressionType::Logical) {
    // Not a logical expression, so for our purposes, we consider it "atomic"
    return;
  }

  // Potentially continuing a chain of logical ANDs/ORs
  const auto& logical_expression = std::static_pointer_cast<LogicalExpression>(chain);
  if (logical_expression->logical_operator != logical_operator) {
    // A logical expression, but not of the right type
    return;
  }

  if (expressions_to_remove.count(logical_expression->left_operand())) {
    chain = logical_expression->right_operand();
    remove_expressions_from_chain(chain, logical_operator, expressions_to_remove);
    return;
  }

  if (expressions_to_remove.count(logical_expression->right_operand())) {
    chain = logical_expression->left_operand();
    remove_expressions_from_chain(chain, logical_operator, expressions_to_remove);
    return;
  }

  remove_expressions_from_chain(logical_expression->left_operand(), logical_operator, expressions_to_remove);
  remove_expressions_from_chain(logical_expression->right_operand(), logical_operator, expressions_to_remove);
}

}  // namespace

bool LogicalExpressionReducerRule::apply_to(const std::shared_ptr<AbstractLQPNode>& node) const {
  auto changed = false;

  MapType previously_reduced_expressions;

  changed |= _apply_to_node(node, previously_reduced_expressions);

  return changed;
}

bool LogicalExpressionReducerRule::_apply_to_node(const std::shared_ptr<AbstractLQPNode>& node,
                                                  MapType& previously_reduced_expressions) const {
  auto changed = false;

  if (node->type == LQPNodeType::Predicate) {
    const auto& predicate_node = std::static_pointer_cast<PredicateNode>(node);
    // It doesn't hurt to have a temporary vector here, because the top-level expression will not be changed anyway, only expressions further down are. This is verified below.
    auto expressions = std::vector<std::shared_ptr<AbstractExpression>>{predicate_node->predicate};
    changed |= _apply_to_expressions(expressions, previously_reduced_expressions);
    DebugAssert(expressions[0] == predicate_node->predicate, "Unexpected change in predicate");
  } else if (node->type == LQPNodeType::Projection) {
    const auto& projection_node = std::static_pointer_cast<ProjectionNode>(node);
    changed |= _apply_to_expressions(projection_node->expressions, previously_reduced_expressions);
  }

  if (node->left_input()) {
    changed |= _apply_to_node(node->left_input(), previously_reduced_expressions);
  }
  if (node->right_input()) {
    changed |= _apply_to_node(node->right_input(), previously_reduced_expressions);
  }

  return changed;
}

bool LogicalExpressionReducerRule::_apply_to_expressions(std::vector<std::shared_ptr<AbstractExpression>>& expressions,
                                                         MapType& previously_reduced_expressions) const {
  auto changed = false;

  for (auto& expression : expressions) {
    std::cout << "\tLooking at expression " << expression->as_column_name() << std::endl;
    visit_expression(expression, [&](auto& subexpression) {
      auto reduced_expression_it = previously_reduced_expressions.find(subexpression);
      if (reduced_expression_it != previously_reduced_expressions.cend()) {
        subexpression = reduced_expression_it->second;
        std::cout << "\tReused." << std::endl;
        return ExpressionVisitation::DoNotVisitArguments;
      }

      if (subexpression->type != ExpressionType::Logical) {
        return ExpressionVisitation::VisitArguments;
      }

      const auto& logical_expression = std::static_pointer_cast<LogicalExpression>(subexpression);
      if (logical_expression->logical_operator == LogicalOperator::Or) {
        ExpressionUnorderedSet or_expressions;
        collect_chained_logical_expressions(subexpression, LogicalOperator::Or, or_expressions);

        std::cout << "\t\tOr expressions: " << std::endl;
        for (const auto& subexpression : or_expressions) {
          std::cout << "\t\t\t- " << subexpression->as_column_name() << std::endl;
        }

        ExpressionUnorderedSet common_and_expressions;
        {
          // Step 1: See if there is anything to do

          auto or_expression_it = or_expressions.begin();
          collect_chained_logical_expressions(*or_expression_it, LogicalOperator::And, common_and_expressions);
          ++or_expression_it;
          while (or_expression_it != or_expressions.end()) {
            std::cout << "\t\tLooking in next OR: " << (*or_expression_it)->as_column_name() << std::endl;
            ExpressionUnorderedSet current_and_expressions;
            collect_chained_logical_expressions(*or_expression_it, LogicalOperator::And, current_and_expressions);
            for (auto and_expression_it = common_and_expressions.begin();
                 and_expression_it != common_and_expressions.end();) {
              std::cout << "\t\t\tTesting: " << (*and_expression_it)->as_column_name() << std::endl;
              if (!current_and_expressions.count(*and_expression_it)) {
                and_expression_it = common_and_expressions.erase(and_expression_it);
              } else {
                ++and_expression_it;
              }
            }
            ++or_expression_it;
          }

          std::cout << "\t\tCommon expressions: " << std::endl;
          for (const auto& subexpression : common_and_expressions) {
            std::cout << "\t\t\t- " << subexpression->as_column_name() << std::endl;
          }

          if (common_and_expressions.empty()) {
            return ExpressionVisitation::DoNotVisitArguments;
          }
        }

        {
          // Step 2: Rewrite the expression

          changed = true;

          auto or_expression_it = or_expressions.cbegin();
          auto new_chain = *or_expression_it;
          remove_expressions_from_chain(new_chain, LogicalOperator::And, common_and_expressions);
          for (; or_expression_it != or_expressions.cend(); ++or_expression_it) {
            auto or_expression = *or_expression_it;  // We want a copy here, because we can't modify the set
            remove_expressions_from_chain(or_expression, LogicalOperator::And, common_and_expressions);
            new_chain = or_(new_chain, or_expression);
          }

          for (const auto& subexpression : common_and_expressions) {
            new_chain = and_(subexpression, new_chain);
          }

          std::cout << "New expression: " << new_chain->as_column_name() << std::endl;

          previously_reduced_expressions.emplace(subexpression, new_chain);
          subexpression = new_chain;
        }
      } else {
        // TODO
      }

      return ExpressionVisitation::DoNotVisitArguments;
    });
  }

  return changed;
}

}  // namespace opossum
