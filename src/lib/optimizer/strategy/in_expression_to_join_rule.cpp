#include "in_expression_to_join_rule.hpp"

#include "logical_query_plan/abstract_lqp_node.hpp"
#include "logical_query_plan/lqp_utils.hpp"
#include "logical_query_plan/static_table_node.hpp"
#include "expression/binary_predicate_expression.hpp"
#include "expression/in_expression.hpp"
#include "expression/list_expression.hpp"
#include "expression/lqp_column_expression.hpp"
#include "expression/value_expression.hpp"
#include "storage/table.hpp"

namespace opossum {
void InExpressionToJoinRule::apply_to(const std::shared_ptr<AbstractLQPNode>& node) const {  // TODO rename to top_node
  std::cout << "apply_to" << std::endl;
  visit_lqp(node, [&](const auto& sub_node) {
    if (sub_node->type != LQPNodeType::Predicate) {
      // This rule only rewrites IN if it is part of a predicate
      return LQPVisitation::VisitInputs;
    }

    const auto& expression = sub_node->node_expressions[0];
    // We only handle top-level INs in this rule. Conjunctive chains (AND) should have been split up by a previous
    // rule, disjunctive chains (OR) don't allow us to extract the IN.
    if (const auto in_expression = std::dynamic_pointer_cast<InExpression>(expression)) {  // TODO change to guard
      // TODO deal with NOT IN
      if (in_expression->set()->type != ExpressionType::List) {
        return LQPVisitation::VisitInputs;
      }
      
      const auto& elements = static_cast<ListExpression&>(*in_expression->set()).elements();
      if (elements[0]->type != ExpressionType::Value) {
        return LQPVisitation::VisitInputs;
      }
      const auto& first_variant = static_cast<ValueExpression&>(*elements[0]).value;

      auto join_node = std::shared_ptr<AbstractLQPNode>{};

      // Resolve element type
      boost::apply_visitor([&](const auto& first_value) {  // TODO could be NULL - test this
        using FirstElementType = std::decay_t<decltype(first_value)>;
        auto values = std::vector<FirstElementType>{};
        values.resize(elements.size());
        values[0] = first_value;

        for (auto element_idx = size_t{1}; element_idx < elements.size(); ++element_idx) {
          if (elements[element_idx]->type != ExpressionType::Value) {
            // Not all elements in the list are a literal value.
            return;
          }

          const auto& nth_variant = static_cast<ValueExpression&>(*elements[element_idx]).value;
          if (nth_variant.which() != first_variant.which()) {
            // Not all elements in the list have the same data type. We could handle NULL here, but
            // `x IN (1, NULL, 3)` does not make sense anyway.
            return;
          }

          values[element_idx] = boost::get<FirstElementType>(nth_variant);
        }


        // TODO extract handling (diamond, expressionevaluator, or disjunction) into separate function
        // TODO For disjunction: Use UnionAll after ensuring duplicates are eliminated - test this especially for floats
        // TODO Remove the old `IN (?)` rule

        const auto list_as_table = 
          std::make_shared<Table>(TableColumnDefinitions{{"value", elements[0]->data_type(), false}}, TableType::Data);
        for (auto& value : values) {
          list_as_table->append({value});
        }
        const auto static_table_node = std::make_shared<StaticTableNode>(list_as_table);
        const auto value_column = std::make_shared<LQPColumnExpression>(LQPColumnReference{static_table_node, ColumnID{0}});
        const auto join_predicate = std::make_shared<BinaryPredicateExpression>(PredicateCondition::Equals, in_expression->value(), value_column);
        join_node = std::make_shared<JoinNode>(JoinMode::Semi, join_predicate);

        lqp_replace_node(sub_node, join_node);
        join_node->set_right_input(static_table_node);
      }, first_variant);

      if (join_node) {
        apply_to(join_node);
        return LQPVisitation::DoNotVisitInputs;
      } else {
        return LQPVisitation::VisitInputs;
      }
    }
    return LQPVisitation::VisitInputs;
  });
}

}
