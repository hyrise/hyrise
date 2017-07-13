#include "node_operator_translator.hpp"

#include <memory>

#include "operators/aggregate.hpp"
#include "operators/get_table.hpp"
#include "operators/projection.hpp"
#include "operators/sort.hpp"
#include "operators/table_scan.hpp"
#include "optimizer/abstract_syntax_tree/aggregate_node.hpp"
#include "optimizer/abstract_syntax_tree/sort_node.hpp"
#include "optimizer/abstract_syntax_tree/table_node.hpp"
#include "optimizer/abstract_syntax_tree/predicate_node.hpp"

namespace opossum {

// singleton
NodeOperatorTranslator &NodeOperatorTranslator::get() {
  static NodeOperatorTranslator instance;
  return instance;
}

NodeOperatorTranslator::NodeOperatorTranslator() {
  _operator_factory[AstNodeType::Table] =
      std::bind(&NodeOperatorTranslator::translate_table_node, this, std::placeholders::_1);
  _operator_factory[AstNodeType::Predicate] =
      std::bind(&NodeOperatorTranslator::translate_table_scan_node, this, std::placeholders::_1);
  _operator_factory[AstNodeType::Projection] =
      std::bind(&NodeOperatorTranslator::translate_projection_node, this, std::placeholders::_1);
  _operator_factory[AstNodeType::Sort] =
      std::bind(&NodeOperatorTranslator::translate_order_by_node, this, std::placeholders::_1);
  _operator_factory[AstNodeType::Aggregate] =
      std::bind(&NodeOperatorTranslator::translate_aggregate_node, this, std::placeholders::_1);
}

std::shared_ptr<AbstractOperator> NodeOperatorTranslator::translate_node(
    std::shared_ptr<AbstractAstNode> node) const {
  auto it = _operator_factory.find(node->type());

  Assert(it != _operator_factory.end(), "No factory for AstNodeType.");

  return it->second(node);
}

std::shared_ptr<AbstractOperator> NodeOperatorTranslator::translate_table_node(
    std::shared_ptr<AbstractAstNode> node) const {
  auto table_node = std::dynamic_pointer_cast<TableNode>(node);
  return std::make_shared<GetTable>(table_node->table_name());
}

std::shared_ptr<AbstractOperator> NodeOperatorTranslator::translate_table_scan_node(
    std::shared_ptr<AbstractAstNode> node) const {
  auto input_operator = translate_node(node->left());
  auto predicate_node = std::dynamic_pointer_cast<PredicateNode>(node);
  return std::make_shared<TableScan>(input_operator, predicate_node->column_name(), predicate_node->scan_type(),
                                     predicate_node->value(), predicate_node->value2());
}

std::shared_ptr<AbstractOperator> NodeOperatorTranslator::translate_projection_node(
    std::shared_ptr<AbstractAstNode> node) const {
  auto input_operator = translate_node(node->left());
  return std::make_shared<Projection>(input_operator, node->output_columns());
}

std::shared_ptr<AbstractOperator> NodeOperatorTranslator::translate_order_by_node(
    std::shared_ptr<AbstractAstNode> node) const {
  auto input_operator = translate_node(node->left());

  auto sort_node = std::dynamic_pointer_cast<SortNode>(node);
  return std::make_shared<Sort>(input_operator, sort_node->column_name(), sort_node->asc());
}

std::shared_ptr<AbstractOperator> NodeOperatorTranslator::translate_aggregate_node(
  std::shared_ptr<AbstractAstNode> node) const {
  auto input_operator = translate_node(node->left());

  const auto aggregate_node = std::dynamic_pointer_cast<AggregateNode>(node);
  const auto & aggregates = aggregate_node->aggregates();

  std::shared_ptr<AbstractOperator> out_operator = input_operator;

  /**
   * Handle arithmetic expressions in aggregate functions via Projection. Support only one level
   * of arithmetics, i.e. SUM(a*b) is fine SUM(a*b+c) is not
   */
  std::vector<std::string> expr_aliases;
  expr_aliases.reserve(aggregates.size());

  Projection::ProjectionDefinitions definitions;
  definitions.reserve(aggregates.size());

  out_operator = std::make_shared<Projection>(out_operator, definitions);

  for (const auto & aggregate : aggregates)
  {
     const auto & expr = aggregate.expr;
    
     Assert(!expr->left() && !expr->right() )
  }

  /**
   * Build Aggregate
   */
  std::vector<std::pair<std::string, AggregateFunction>> aggregate_definitions;
  aggregate_definitions.reserve(aggregates.size());
  for (size_t aggregate_idx = 0; aggregate_idx < aggregates.size(); aggregate_idx++) {
    const auto & aggregate = aggregates[aggregate_idx];

    Assert(aggregate.expr->expression_type() == ExpressionType::ExpressionFunctionReference, ""
      "Only functions are supported in Aggregates");
    const auto aggregate_function = aggregate.expr->aggregate_function();

    aggregate_definitions.emplace_back(expr_aliases[aggregate_idx], aggregate_function);
  }
  out_operator = std::make_shared<Aggregate>(out_operator, aggregate_definitions);

  /**
   * Build Projection from Aggregate functions to alias names
   *    e.g. for `SUM(a*b) as foo` this will project the column "SUM(a*b)" to "foo"
   */
  auto alias_projection_needed = std::any_of(aggregates.begin(), aggregates.end(),
                                             [] (const auto & aggregate) {
    return static_cast<bool>(aggregate.alias);
  });

  // If there are no aliases just skip this step, e.g. for SELECT COUNT(), SUM(a) [...]
  // this step is not necessary.
  if (alias_projection_needed) {
    std::vector<std::string> columns;
    columns.reserve(aggregates.size());

    for (const auto & aggregate : aggregates) {
      std::string out_alias;
      if (aggregate.alias) {
        out_alias = *aggregate.alias;
      } else {
        // If there is no alias for a column, just reproject to the same name, e.g.,
        // in SELECT COUNT() as c, SUM(a) [...] this will project "SUM(a)" to "SUM(a)"
        out_alias = aggregate.expr->to_alias_name();
      }

      columns.emplace_back(out_alias);
    }

    out_operator = std::make_shared<Projection>(out_operator, columns);
  }
}

}  // namespace opossum
