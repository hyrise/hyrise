#include "jit_aware_lqp_translator.hpp"

#include <boost/range/adaptors.hpp>
#include <boost/range/combine.hpp>

#include <queue>
#include <unordered_set>

#include "constant_mappings.hpp"
#include "logical_query_plan/aggregate_node.hpp"
#include "logical_query_plan/projection_node.hpp"
#include "logical_query_plan/predicate_node.hpp"
#include "logical_query_plan/stored_table_node.hpp"
#include "operators/operator_scan_predicate.hpp"
#include "operators/jit_aggregate.hpp"
#include "operators/jit_compute.hpp"
#include "operators/jit_filter.hpp"
#include "operators/jit_read_tuples.hpp"
#include "operators/jit_write_tuples.hpp"
#include "storage/storage_manager.hpp"

namespace opossum {

JitAwareLQPTranslator::JitAwareLQPTranslator() : LQPTranslator() {
#if !HYRISE_JIT_SUPPORT
  Fail("Query translation with JIT operators requested, but jitting is not available");
#endif
}

std::shared_ptr<AbstractOperator> JitAwareLQPTranslator::translate_node(
    const std::shared_ptr<AbstractLQPNode>& node) const {
  auto jit_operator = _try_translate_node_to_jit_operators(node);
  return jit_operator ? jit_operator : LQPTranslator::translate_node(node);
}

std::shared_ptr<JitOperatorWrapper> JitAwareLQPTranslator::_try_translate_node_to_jit_operators(
    const std::shared_ptr<AbstractLQPNode>& node) const {
  uint32_t num_jittable_nodes{0};
  std::unordered_set<std::shared_ptr<AbstractLQPNode>> input_nodes;

  // Traverse query tree until a non-jittable nodes is found in each branch
  _visit(node, [&](auto& current_node) {
    const auto is_root_node = current_node == node;
    if (_node_is_jittable(current_node, is_root_node)) {
      ++num_jittable_nodes;
      return true;
    } else {
      input_nodes.insert(current_node);
      return false;
    }
  });

  // We use a really simple heuristic to decide when to introduce jittable operators:
  // For aggregate operations replacing a single AggregateNode with a JitAggregate operator already yields significant
  // runtime benefits. Otherwise, it does not make sense to create a JitOperatorWrapper for fewer than 2 LQP nodes.
  if (num_jittable_nodes < 1 || (num_jittable_nodes < 2 && node->type != LQPNodeType::Aggregate) ||
      input_nodes.size() != 1) {
    return nullptr;
  }

  // The input_node is not being integrated into the operator chain, but instead serves as the input to the JitOperators
  const auto input_node = *input_nodes.begin();

  auto jit_operator = std::make_shared<JitOperatorWrapper>(translate_node(input_node));
  auto read_tuple = std::make_shared<JitReadTuples>();
  jit_operator->add_jit_operator(read_tuple);

  auto filter_node = node;
  while (filter_node != input_node && filter_node->type != LQPNodeType::Predicate &&
         filter_node->type != LQPNodeType::Union) {
    filter_node = filter_node->left_input();
  }

  // If we can reach the input node without encountering a UnionNode or PredicateNode,
  // there is no need to filter any tuples
  if (filter_node != input_node) {
    // However, if we do need to filter, we first convert the filter node to a JitExpression ...
    const auto expression = _try_translate_node_to_jit_expression(filter_node, *read_tuple, input_node);
    if (!expression) return nullptr;
    // make sure that the expression gets computed ...
    jit_operator->add_jit_operator(std::make_shared<JitCompute>(expression));
    // and then filter on the resulting boolean.
    jit_operator->add_jit_operator(std::make_shared<JitFilter>(expression->result()));
  }

  if (node->type == LQPNodeType::Aggregate) {
    // Since aggregate nodes cause materialization, there is at most one JitAggregate operator in each operator chain
    // and it must be the last operator of the chain. The _node_is_jittable function takes care of this by rejecting
    // aggregate nodes that would be placed in the middle of an operator chain.
    const auto aggregate_node = std::static_pointer_cast<AggregateNode>(node);

    auto aggregate = std::make_shared<JitAggregate>();
    const auto& aggregate_columns = aggregate_node->aggregate_expressions;

    for (const auto& groupby_expression : aggregate_node->group_by_expressions) {
      const auto jit_expression = _try_translate_column_to_jit_expression(*groupby_expression, *read_tuple, input_node);
      if (!jit_expression) return nullptr;
      // Create a JitCompute operator for each computed groupby column ...
      if (jit_expression->expression_type() != JitExpressionType::Column) {
        jit_operator->add_jit_operator(std::make_shared<JitCompute>(jit_expression));
      }
      // ... and add the column to the JitAggregate operator.
      aggregate->add_groupby_column(groupby_expression->as_column_name(), jit_expression->result());
    }

    for (const auto& expression : aggregate_node->aggregate_expressions) {
      const auto aggregate_expression = std::dynamic_pointer_cast<AggregateExpression>(expression);
      DebugAssert(aggregate_expression->type == ExpressionType::Aggregate, "Expression is not a function.");

      const auto expression = _try_translate_expression_to_jit_expression(
          *aggregate_expression->arguments()[0], *read_tuple, input_node);
      if (!expression) return nullptr;
      // Create a JitCompute operator for each aggregate expression on a computed value ...
      if (expression->expression_type() != JitExpressionType::Column) {
        jit_operator->add_jit_operator(std::make_shared<JitCompute>(expression));
      }
      // ... and add the aggregate expression to the JitAggregate operator.
      aggregate->add_aggregate_column(aggregate_expression->as_column_name(), expression->result(),
                                      aggregate_expression->);
    }

    jit_operator->add_jit_operator(aggregate);
  } else {
    // Add a compute operator for each computed output column (i.e., a column that is not from a stored table).
    auto write_table = std::make_shared<JitWriteTuples>();
    for (const auto& column_expression : node->column_expressions()) {
      const auto jit_expression = _try_translate_column_to_jit_expression(*column_expression, *read_tuple, input_node);
      if (!jit_expression) return nullptr;
      // If the JitExpression is of type JitExpressionType::Column, there is no need to add a compute node, since it
      // would not compute anything anyway
      if (jit_expression->expression_type() != JitExpressionType::Column) {
        jit_operator->add_jit_operator(std::make_shared<JitCompute>(jit_expression));
      }
      write_table->add_output_column(column_expression->as_column_name(), jit_expression->result());
    }

    jit_operator->add_jit_operator(write_table);
  }

  return jit_operator;
}

std::shared_ptr<const JitExpression> JitAwareLQPTranslator::_try_translate_node_to_jit_expression(
    const std::shared_ptr<AbstractLQPNode>& node, JitReadTuples& jit_source,
    const std::shared_ptr<AbstractLQPNode>& input_node) const {
  std::shared_ptr<const JitExpression> left, right;
  switch (node->type()) {
    case LQPNodeType::Predicate:
      // If the PredicateNode has further conditions (either PredicateNodes or UnionNodes) as children, we need a
      // logical AND here. In this case, the condition represented by the PredicateNode itself becomes the left side
      // and the input to the predicate node becomes the right side of the conjunction.
      // If the PredicateNode has no further conditions (i.e., all tuples pass into the PredicateNode unconditionally),
      // there is no need to create a dedicated AND-expression.
      if (_input_is_filtered(node->left_input())) {
        left = _try_translate_node_to_jit_expression(node->left_input(), jit_source, input_node);
        right = _try_translate_predicate_to_jit_expression(std::dynamic_pointer_cast<PredicateNode>(node), jit_source,
                                                           input_node);
        return left && right
                   ? std::make_shared<JitExpression>(left, JitExpressionType::And, right, jit_source.add_temporary_value())
                   : nullptr;
      } else {
        return _try_translate_predicate_to_jit_expression(std::dynamic_pointer_cast<PredicateNode>(node), jit_source,
                                                          input_node);
      }

    case LQPNodeType::Union:
      left = _try_translate_node_to_jit_expression(node->left_input(), jit_source, input_node);
      right = _try_translate_node_to_jit_expression(node->right_input(), jit_source, input_node);
      return left && right
                 ? std::make_shared<JitExpression>(left, JitExpressionType::Or, right, jit_source.add_temporary_value())
                 : nullptr;

    case LQPNodeType::Projection:
      // We don't care about projection nodes here, since they do not perform any tuple filtering
      return _try_translate_node_to_jit_expression(node->left_input(), jit_source, input_node);

    default:
      return nullptr;
  }
}

std::shared_ptr<const JitExpression> JitAwareLQPTranslator::_try_translate_predicate_to_jit_expression(
    const std::shared_ptr<PredicateNode>& node, JitReadTuples& jit_source,
    const std::shared_ptr<AbstractLQPNode>& input_node) const {
  auto condition = predicate_condition_to_expression_type.at(node->predicate_condition());
  auto left = _try_translate_column_to_jit_expression(node->column_reference(), jit_source, input_node);
  if (!left) return nullptr;
  std::shared_ptr<const JitExpression> right;

  switch (condition) {
    /* Binary predicates */
    case JitExpressionType::Equals:
    case JitExpressionType::NotEquals:
    case JitExpressionType::LessThan:
    case JitExpressionType::LessThanEquals:
    case JitExpressionType::GreaterThan:
    case JitExpressionType::GreaterThanEquals:
    case JitExpressionType::Like:
    case JitExpressionType::NotLike:
      right = _try_translate_variant_to_jit_expression(node->value(), jit_source, input_node);
      return right ? std::make_shared<JitExpression>(left, condition, right, jit_source.add_temporary_value())
                   : nullptr;
    /* Unary predicates */
    case JitExpressionType::IsNull:
    case JitExpressionType::IsNotNull:
      return std::make_shared<JitExpression>(left, condition, jit_source.add_temporary_value());
    default:
      return nullptr;
  }
}

std::shared_ptr<const JitExpression> JitAwareLQPTranslator::_try_translate_expression_to_jit_expression(
    const AbstractExpression& lqp_expression, JitReadTuples& jit_source,
    const std::shared_ptr<AbstractLQPNode>& input_node) const {
  std::shared_ptr<const JitExpression> left, right;
  switch (lqp_expression.type()) {
    case JitExpressionType::Literal:
      return _try_translate_variant_to_jit_expression(lqp_expression.value(), jit_source, input_node);

    case JitExpressionType::Column:
      return _try_translate_column_to_jit_expression(lqp_expression.column_reference(), jit_source, input_node);

    /* Binary operators */
    case JitExpressionType::Addition:
    case JitExpressionType::Subtraction:
    case JitExpressionType::Multiplication:
    case JitExpressionType::Division:
    case JitExpressionType::Modulo:
    case JitExpressionType::Power:
    case JitExpressionType::Equals:
    case JitExpressionType::NotEquals:
    case JitExpressionType::LessThan:
    case JitExpressionType::LessThanEquals:
    case JitExpressionType::GreaterThan:
    case JitExpressionType::GreaterThanEquals:
    case JitExpressionType::Like:
    case JitExpressionType::NotLike:
    case JitExpressionType::And:
    case JitExpressionType::Or:
      left = _try_translate_expression_to_jit_expression(*lqp_expression.left_child(), jit_source, input_node);
      right = _try_translate_expression_to_jit_expression(*lqp_expression.right_child(), jit_source, input_node);
      return std::make_shared<JitExpression>(left, lqp_expression.type(), right, jit_source.add_temporary_value());

    /* Unary operators */
    case JitExpressionType::Not:
    case JitExpressionType::IsNull:
    case JitExpressionType::IsNotNull:
      left = _try_translate_expression_to_jit_expression(*lqp_expression.left_child(), jit_source, input_node);
      return std::make_shared<JitExpression>(left, lqp_expression.type(), jit_source.add_temporary_value());

    default:
      return nullptr;
  }
}

std::shared_ptr<const JitExpression> JitAwareLQPTranslator::_try_translate_column_to_jit_expression(
    const LQPColumnReference& lqp_column_reference, JitReadTuples& jit_source,
    const std::shared_ptr<AbstractLQPNode>& input_node) const {
  const auto column_id = input_node->find_output_column_id(lqp_column_reference);
  const auto stored_table_node = std::dynamic_pointer_cast<const StoredTableNode>(lqp_column_reference.original_node());
  const auto projection_node = std::dynamic_pointer_cast<const ProjectionNode>(lqp_column_reference.original_node());

  if (column_id && stored_table_node) {
    // we reached a "raw" data column and register it as an input column
    const auto original_column_id = lqp_column_reference.original_column_id();
    const auto table_name = stored_table_node->table_name();
    const auto table = StorageManager::get().get_table(table_name);
    const auto data_type = table->column_data_type(original_column_id);
    const auto is_nullable = table->column_is_nullable(original_column_id);
    const auto tuple_value = jit_source.add_input_column(data_type, is_nullable, column_id.value());
    return std::make_shared<JitExpression>(tuple_value);
  } else if (projection_node) {
    // if the LQPColumnReference references a computed column, we need to compute that expression as well
    const auto lqp_expression = projection_node->column_expressions()[lqp_column_reference.original_column_id()];
    return _try_translate_expression_to_jit_expression(*lqp_expression, jit_source, input_node);
  } else {
    return nullptr;
  }
}

std::shared_ptr<const JitExpression> JitAwareLQPTranslator::_try_translate_variant_to_jit_expression(
    const AllParameterVariant& value, JitReadTuples& jit_source,
    const std::shared_ptr<AbstractLQPNode>& input_node) const {
  if (is_lqp_column_reference(value)) {
    return _try_translate_column_to_jit_expression(boost::get<LQPColumnReference>(value), jit_source, input_node);
  } else if (is_variant(value)) {
    const auto variant = boost::get<AllTypeVariant>(value);
    const auto tuple_value = jit_source.add_literal_value(variant);
    return std::make_shared<JitExpression>(tuple_value);
  } else {
    return nullptr;
  }
}

bool JitAwareLQPTranslator::_input_is_filtered(const std::shared_ptr<AbstractLQPNode>& node) const {
  auto current_node = node;
  while (current_node->type() == LQPNodeType::Projection) {
    current_node = current_node->left_input();
  }
  return current_node->type() == LQPNodeType::Predicate || current_node->type() == LQPNodeType::Union;
}

bool JitAwareLQPTranslator::_node_is_jittable(const std::shared_ptr<AbstractLQPNode>& node,
                                              const bool allow_aggregate_node) const {
  if (node->type == LQPNodeType::Aggregate) {
    // We do not support the count distinct function yet and thus need to check all aggregate expressions.
    auto aggregate_node = std::static_pointer_cast<AggregateNode>(node);
    auto aggregate_expressions = aggregate_node->aggregate_expressions();
    auto has_count_distict = std::count_if(
        aggregate_expressions.begin(), aggregate_expressions.end(),
        [](auto& expression) { return expression->aggregate_function() == AggregateFunction::CountDistinct; });
    return allow_aggregate_node && !has_count_distict;
  }

  if (node->type == LQPNodeType::Predicate) {
    auto predicate_node = std::static_pointer_cast<PredicateNode>(node);

    const auto operator_scan_predicates = OperatorScanPredicate::from_expression(*predicate_node->predicate, *predicate_node);

    // The JIT doesn't support Between
    const auto is_not_between = operator_scan_predicates &&
    operator_scan_predicates->size() == 1 &&
                                operator_scan_predicates->front().predicate_condition != PredicateCondition::Between;

    return predicate_node->scan_type == ScanType::TableScan && is_not_between;
  }

  return node->type == LQPNodeType::Projection || node->type == LQPNodeType::Union;   
}

void JitAwareLQPTranslator::_visit(const std::shared_ptr<AbstractLQPNode>& node,
                                   std::function<bool(const std::shared_ptr<AbstractLQPNode>&)> func) const {
  std::unordered_set<std::shared_ptr<const AbstractLQPNode>> visited;
  std::queue<std::shared_ptr<AbstractLQPNode>> queue({node});

  while (!queue.empty()) {
    auto current_node = queue.front();
    queue.pop();

    if (!current_node || visited.count(current_node)) {
      continue;
    }
    visited.insert(current_node);

    if (func(current_node)) {
      queue.push(current_node->left_input());
      queue.push(current_node->right_input());
    }
  }
}

}  // namespace opossum
