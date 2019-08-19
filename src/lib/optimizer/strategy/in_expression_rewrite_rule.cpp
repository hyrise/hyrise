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
#include "storage/table.hpp"

namespace {
using namespace opossum;                         // NOLINT
using namespace opossum::expression_functional;  // NOLINT

void rewrite_to_join(const std::shared_ptr<AbstractLQPNode>& node,
                     const std::shared_ptr<AbstractExpression>& left_value,
                     const std::vector<std::shared_ptr<AbstractExpression>>& elements, DataType data_type) {
  // TODO test NULL
  auto right_values = std::vector<AllTypeVariant>{};
  right_values.reserve(elements.size());

  for (const auto& element : elements) {
    const auto& variant = static_cast<ValueExpression&>(*element).value;
    right_values.emplace_back(variant);
  }

  // TODO Remove the old `IN (?)` rule

  const auto list_as_table =
      std::make_shared<Table>(TableColumnDefinitions{{"right_values", data_type, true}}, TableType::Data);

  for (auto& right_value : right_values) {
    list_as_table->append({right_value});
  }

  const auto static_table_node = std::make_shared<StaticTableNode>(list_as_table);
  const auto list_column = std::make_shared<LQPColumnExpression>(LQPColumnReference{static_table_node, ColumnID{0}});
  const auto join_predicate =
      std::make_shared<BinaryPredicateExpression>(PredicateCondition::Equals, left_value, list_column);
  auto join_node = std::make_shared<JoinNode>(JoinMode::Semi, join_predicate);

  lqp_replace_node(node, join_node);
  join_node->set_right_input(static_table_node);
}

void rewrite_to_disjunction(const std::shared_ptr<AbstractLQPNode>& node,
                            const std::shared_ptr<AbstractExpression>& left_value,
                            const std::vector<std::shared_ptr<AbstractExpression>>& elements, DataType data_type) {
  const auto old_output_relations = node->output_relations();

  std::vector<std::shared_ptr<PredicateNode>> predicate_nodes{};
  predicate_nodes.reserve(elements.size());

  // TOOD eliminate duplicates and test this for floats
  for (const auto& element : elements) {
    auto predicate_node = PredicateNode::make(equals_(left_value, element), node->left_input());
    predicate_nodes.push_back(std::move(predicate_node));
  }

  std::shared_ptr<AbstractLQPNode> last_node = predicate_nodes[0];
  for (auto predicate_node_idx = size_t{1}; predicate_node_idx < predicate_nodes.size(); ++predicate_node_idx) {
    last_node = UnionNode::make(UnionMode::All, last_node, predicate_nodes[predicate_node_idx]);
  }

  for (const auto& [output, input_side] : old_output_relations) {
    output->set_input(input_side, last_node);
  }
}

}  // namespace

namespace opossum {

void InExpressionRewriteJoinRule::apply_to(
    const std::shared_ptr<AbstractLQPNode>& node) const {  // TODO rename to top_node
  if (forced_algorithm == Algorithm::ExpressionEvaluator) return;

  visit_lqp(node, [&](const auto& sub_node) {
    if (sub_node->type != LQPNodeType::Predicate) {
      // This rule only rewrites IN if it is part of a predicate
      return LQPVisitation::VisitInputs;
    }

    const auto& expression = sub_node->node_expressions[0];
    // We only handle top-level INs in this rule. Conjunctive chains (AND) should have been split up by a previous
    // rule, disjunctive chains (OR) don't allow us to extract the IN.

    const auto in_expression = std::dynamic_pointer_cast<InExpression>(expression);
    if (!in_expression || (in_expression->set()->type != ExpressionType::List)) {
      return LQPVisitation::VisitInputs;
    }
    // TODO deal with NOT IN

    const auto& elements = static_cast<ListExpression&>(*in_expression->set()).elements();

    // Check whether we can express the InExpression as a join. This only works if all elements are literal values of
    // the same data type. If all values are NULL, we also cannot handle the IN with a join.
    std::optional<DataType> common_data_type;
    for (const auto& element : elements) {
      if (element->type != ExpressionType::Value) {
        common_data_type = std::nullopt;
        break;
      }
      const auto& value_expression = static_cast<ValueExpression&>(*element);
      if (variant_is_null(value_expression.value)) continue;
      if (!common_data_type) {
        common_data_type = value_expression.data_type();
      } else {
        if (value_expression.data_type() != *common_data_type) {
          common_data_type = std::nullopt;
          break;
        }
      }
    }

    if (common_data_type && forced_algorithm == Algorithm::Join) {
      // TODO(anyone): In the future, we could also check for applicable indexes
      rewrite_to_join(sub_node, in_expression->value(), elements, *common_data_type);
    } else if (forced_algorithm ==
               Algorithm::Disjunction) {  // TODO - maybe stick to expression evaluator if we need it anyway?
      rewrite_to_disjunction(sub_node, in_expression->value(), elements, *common_data_type);
    } else if (forced_algorithm == Algorithm::Auto) {
      if (elements.size() < 3) {
        // Also keeps plan visualizations from becoming messy
        rewrite_to_disjunction(sub_node, in_expression->value(), elements, *common_data_type);
      } else if (common_data_type && elements.size() > 500) {
        rewrite_to_join(sub_node, in_expression->value(), elements, *common_data_type);
      }
    }

    return LQPVisitation::VisitInputs;
  });
}

}  // namespace opossum
