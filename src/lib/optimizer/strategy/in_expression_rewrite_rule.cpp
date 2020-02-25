#include "in_expression_rewrite_rule.hpp"

#include "expression/binary_predicate_expression.hpp"
#include "expression/expression_functional.hpp"
#include "expression/in_expression.hpp"
#include "expression/list_expression.hpp"
#include "expression/lqp_column_expression.hpp"
#include "expression/value_expression.hpp"
#include "logical_query_plan/abstract_lqp_node.hpp"
#include "logical_query_plan/lqp_utils.hpp"
#include "logical_query_plan/static_table_node.hpp"
#include "logical_query_plan/union_node.hpp"
#include "statistics/table_statistics.hpp"
#include "storage/table.hpp"

namespace {
using namespace opossum;                         // NOLINT
using namespace opossum::expression_functional;  // NOLINT

void rewrite_to_join(const std::shared_ptr<AbstractLQPNode>& node,
                     const std::shared_ptr<AbstractExpression>& left_expression,
                     const std::vector<std::shared_ptr<AbstractExpression>>& right_side_expressions, DataType data_type,
                     const bool is_negated) {
  // Create the temporary table for the build side of the semi/anti join
  const auto list_as_table =
      std::make_shared<Table>(TableColumnDefinitions{{"right_values", data_type, false}}, TableType::Data);

  // Fill the temporary table with values
  resolve_data_type(data_type, [&](const auto data_type_t) {
    using ColumnDataType = typename decltype(data_type_t)::type;
    auto right_values = pmr_concurrent_vector<ColumnDataType>{};
    right_values.reserve(right_side_expressions.size());

    for (const auto& element : right_side_expressions) {
      if (variant_is_null(static_cast<ValueExpression&>(*element).value)) {
        // Null values on the right side will not lead to a match, anyway
        continue;
      }

      right_values.push_back(boost::get<ColumnDataType>(static_cast<ValueExpression&>(*element).value));
    }

    const auto value_segment = std::make_shared<ValueSegment<ColumnDataType>>(std::move(right_values));
    list_as_table->append_chunk({value_segment});
  });

  // Add statistics to the dummy table so that following rules or other steps (such as the LQPVisualizer), which
  // expect statistics to be present, do not run into problems.
  list_as_table->set_table_statistics(TableStatistics::from_table(*list_as_table));

  // Create a join node
  const auto static_table_node = std::make_shared<StaticTableNode>(list_as_table);
  const auto right_column = std::make_shared<LQPColumnExpression>(LQPColumnReference{static_table_node, ColumnID{0}});
  const auto join_predicate =
      std::make_shared<BinaryPredicateExpression>(PredicateCondition::Equals, left_expression, right_column);
  const auto join_mode = is_negated ? JoinMode::AntiNullAsTrue : JoinMode::Semi;
  auto join_node = std::make_shared<JoinNode>(join_mode, join_predicate);

  // Replace the IN predicate with the join node
  lqp_replace_node(node, join_node);
  join_node->set_right_input(static_table_node);
}

void rewrite_to_disjunction(const std::shared_ptr<AbstractLQPNode>& node,
                            const std::shared_ptr<AbstractExpression>& left_expression,
                            const std::vector<std::shared_ptr<AbstractExpression>>& right_side_expressions,
                            DataType data_type) {
  // It is easier not to use lqp_replace_node here, so we need to cache the original output relations
  const auto old_output_relations = node->output_relations();

  std::vector<std::shared_ptr<PredicateNode>> predicate_nodes{};
  predicate_nodes.reserve(right_side_expressions.size());

  // Remove duplicates so that the results of the predicates do not overlap. Otherwise, rows might get added twice.
  // Using an ExpressionUnorderedSet here means that the order of predicates is undefined.
  auto unique_right_side_expressions =
      ExpressionUnorderedSet{right_side_expressions.begin(), right_side_expressions.end()};
  for (const auto& element : unique_right_side_expressions) {
    auto predicate_node = PredicateNode::make(equals_(left_expression, element), node->left_input());
    predicate_nodes.push_back(std::move(predicate_node));
  }

  // Create a PredicateNode for the first value. Then, successively hook up additional PredicateNodes using UnionNodes.
  std::shared_ptr<AbstractLQPNode> last_node = predicate_nodes[0];
  for (auto predicate_node_idx = size_t{1}; predicate_node_idx < predicate_nodes.size(); ++predicate_node_idx) {
    last_node = UnionNode::make(UnionMode::All, last_node, predicate_nodes[predicate_node_idx]);
  }

  // Attach the final UnionNode (or PredicateNode if only one) to the original plan. As this replaces the original
  // input(s), the IN PredicateNode is deleted automatically.
  for (const auto& [output, input_side] : old_output_relations) {
    output->set_input(input_side, last_node);
  }
}

}  // namespace

namespace opossum {

void InExpressionRewriteRule::apply_to(const std::shared_ptr<AbstractLQPNode>& node) const {
  if (strategy == Strategy::ExpressionEvaluator) {
    // This is the default anyway, i.e., what the SQLTranslator gave us
    return;
  }

  visit_lqp(node, [&](const auto& sub_node) {
    if (sub_node->type != LQPNodeType::Predicate) {
      // This rule only rewrites IN if it is part of a predicate (not, e.g., `SELECT a IN (1, 2) AS foo`)
      return LQPVisitation::VisitInputs;
    }

    const auto& expression = sub_node->node_expressions[0];
    const auto in_expression = std::dynamic_pointer_cast<InExpression>(expression);

    // We only handle top-level INs in this rule. Conjunctive chains (AND) should have been split up by a previous
    // rule, disjunctive chains (OR) don't allow us to extract the IN into a different predicate. Also, the right
    // side has to be a list, not a subquery. Those are handled by the SubqueryToJoinRule.
    if (!in_expression || (in_expression->set()->type != ExpressionType::List)) {
      return LQPVisitation::VisitInputs;
    }

    const auto& left_expression = in_expression->value();
    const auto& right_side_expressions = static_cast<ListExpression&>(*in_expression->set()).elements();

    // Check whether all elements are literal values of the same data type (that is not NULL).
    std::optional<DataType> common_data_type = left_expression->data_type();
    for (const auto& element : right_side_expressions) {
      if (element->type != ExpressionType::Value) {
        common_data_type = std::nullopt;
        break;
      }
      const auto& value_expression = static_cast<ValueExpression&>(*element);
      if (variant_is_null(value_expression.value)) continue;
      if (value_expression.data_type() != *common_data_type) {
        common_data_type = std::nullopt;
        break;
      }
    }

    if (!common_data_type) {
      // Disjunctive predicates could theoretically handle differing types, but that makes it harder to eliminate
      // duplicates. Let the ExpressionEvaluator handle this rare case.

      Assert(strategy == Strategy::Auto || strategy == Strategy::ExpressionEvaluator,
             "Could not apply strategy as types mismatch");

      return LQPVisitation::VisitInputs;
    }

    if (strategy == Strategy::Join) {
      rewrite_to_join(sub_node, left_expression, right_side_expressions, *common_data_type,
                      in_expression->is_negated());
    } else if (strategy == Strategy::Disjunction) {
      Assert(!in_expression->is_negated(), "Disjunctions cannot handle NOT IN");
      rewrite_to_disjunction(sub_node, left_expression, right_side_expressions, *common_data_type);
    } else if (strategy == Strategy::Auto) {
      if (right_side_expressions.size() <= MAX_ELEMENTS_FOR_DISJUNCTION && !in_expression->is_negated()) {
        rewrite_to_disjunction(sub_node, left_expression, right_side_expressions, *common_data_type);
      } else if (common_data_type && right_side_expressions.size() >= MIN_ELEMENTS_FOR_JOIN) {
        rewrite_to_join(sub_node, left_expression, right_side_expressions, *common_data_type,
                        in_expression->is_negated());
      } else {
        // Stick with the ExpressionEvaluator
      }
    }

    return LQPVisitation::VisitInputs;
  });
}

}  // namespace opossum
