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

// TODO measure with TPC-DS query 8

namespace {
using namespace opossum;                         // NOLINT
using namespace opossum::expression_functional;  // NOLINT

void rewrite_to_join(const std::shared_ptr<AbstractLQPNode>& node,
                     const std::shared_ptr<AbstractExpression>& left_value,
                     const std::vector<std::shared_ptr<AbstractExpression>>& elements, DataType data_type,
                     const bool is_negated) {
  const auto list_as_table =
      std::make_shared<Table>(TableColumnDefinitions{{"right_values", data_type, false}}, TableType::Data);

  resolve_data_type(data_type, [&](const auto data_type_t) {
    using ColumnDataType = typename decltype(data_type_t)::type;
    auto right_values = pmr_concurrent_vector<ColumnDataType>{};
    right_values.reserve(elements.size());

    for (const auto& element : elements) {
      if (variant_is_null(static_cast<ValueExpression&>(*element).value)) {
        // Null values on the right side will not lead to a match, anyway
        continue;
      }

      right_values.emplace_back(boost::get<ColumnDataType>(static_cast<ValueExpression&>(*element).value));
    }

    const auto value_segment = std::make_shared<ValueSegment<ColumnDataType>>(std::move(right_values));
    list_as_table->append_chunk({value_segment});
  });

  const auto static_table_node = std::make_shared<StaticTableNode>(list_as_table);
  const auto list_column = std::make_shared<LQPColumnExpression>(LQPColumnReference{static_table_node, ColumnID{0}});
  const auto join_predicate =
      std::make_shared<BinaryPredicateExpression>(PredicateCondition::Equals, left_value, list_column);
  const auto join_mode = is_negated ? JoinMode::AntiNullAsTrue : JoinMode::Semi;
  auto join_node = std::make_shared<JoinNode>(join_mode, join_predicate);

  lqp_replace_node(node, join_node);
  join_node->set_right_input(static_table_node);
}

void rewrite_to_disjunction(const std::shared_ptr<AbstractLQPNode>& node,
                            const std::shared_ptr<AbstractExpression>& left_value,
                            const std::vector<std::shared_ptr<AbstractExpression>>& elements, DataType data_type) {
  const auto old_output_relations = node->output_relations();

  std::vector<std::shared_ptr<PredicateNode>> predicate_nodes{};
  predicate_nodes.reserve(elements.size());

  // Remove duplicates - otherwise UnionMode::All would add a row that matches two values in the element list twice
  // Using an ExpressionUnorderedSet here means that the order of predicates is undefined.
  auto unique_elements = ExpressionUnorderedSet{elements.begin(), elements.end()};
  for (const auto& element : unique_elements) {
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

void InExpressionRewriteRule::apply_to(const std::shared_ptr<AbstractLQPNode>& node) const {
  if (strategy == Strategy::ExpressionEvaluator) {
    // This is the default anyway, i.e., what the SQLTranslator gave us
    return;
  }

  // Gather the values and some information about the strategies that we could use
  visit_lqp(node, [&](const auto& sub_node) {
    if (sub_node->type != LQPNodeType::Predicate) {
      // This rule only rewrites IN if it is part of a predicate (not, e.g., `SELECT a IN (1, 2 ) AS foo`)
      return LQPVisitation::VisitInputs;
    }

    const auto& expression = sub_node->node_expressions[0];
    // We only handle top-level INs in this rule. Conjunctive chains (AND) should have been split up by a previous
    // rule, disjunctive chains (OR) don't allow us to extract the IN into a different predicate.

    const auto in_expression = std::dynamic_pointer_cast<InExpression>(expression);
    if (!in_expression || (in_expression->set()->type != ExpressionType::List)) {
      return LQPVisitation::VisitInputs;
    }

    const auto& elements = static_cast<ListExpression&>(*in_expression->set()).elements();

    // Check whether all elements are literal values of the same data type (that is not NULL).
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

    if (!common_data_type) {
      // Disjunctive predicates could theoretically handle differing types, but that makes it hard to elimated
      // duplicates. Better be safe and let the ExpressionEvaluator handle this rare case.

      Assert(strategy == Strategy::Auto || strategy == Strategy::ExpressionEvaluator,
             "Could not apply strategy as types mismatch");

      return LQPVisitation::VisitInputs;
    }

    if (strategy == Strategy::Join) {
      rewrite_to_join(sub_node, in_expression->value(), elements, *common_data_type, in_expression->is_negated());
    } else if (strategy == Strategy::Disjunction) {
      Assert(!in_expression->is_negated(), "Disjunctions cannot handle NOT IN");
      rewrite_to_disjunction(sub_node, in_expression->value(), elements, *common_data_type);
    } else if (strategy == Strategy::Auto) {
      if (elements.size() < MAX_ELEMENTS_FOR_DISJUNCTION && !in_expression->is_negated()) {
        // Also keeps plan visualizations from becoming messy
        rewrite_to_disjunction(sub_node, in_expression->value(), elements, *common_data_type);
      } else if (common_data_type && elements.size() > MIN_ELEMENTS_FOR_JOIN) {
        rewrite_to_join(sub_node, in_expression->value(), elements, *common_data_type, in_expression->is_negated());
      } else {
        // Stick with the ExpressionEvaluator
      }
    }

    return LQPVisitation::VisitInputs;
  });
}

}  // namespace opossum
