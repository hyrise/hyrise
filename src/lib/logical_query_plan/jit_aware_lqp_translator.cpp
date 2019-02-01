#include "jit_aware_lqp_translator.hpp"

#if HYRISE_JIT_SUPPORT

#include <boost/range/adaptors.hpp>
#include <boost/range/combine.hpp>

#include <queue>
#include <unordered_set>

#include "constant_mappings.hpp"
#include "expression/abstract_predicate_expression.hpp"
#include "expression/arithmetic_expression.hpp"
#include "expression/logical_expression.hpp"
#include "expression/lqp_column_expression.hpp"
#include "expression/value_expression.hpp"
#include "logical_query_plan/aggregate_node.hpp"
#include "logical_query_plan/limit_node.hpp"
#include "logical_query_plan/lqp_utils.hpp"
#include "logical_query_plan/predicate_node.hpp"
#include "logical_query_plan/projection_node.hpp"
#include "logical_query_plan/stored_table_node.hpp"
#include "operators/jit_operator/operators/jit_aggregate.hpp"
#include "operators/jit_operator/operators/jit_compute.hpp"
#include "operators/jit_operator/operators/jit_filter.hpp"
#include "operators/jit_operator/operators/jit_limit.hpp"
#include "operators/jit_operator/operators/jit_read_tuples.hpp"
#include "operators/jit_operator/operators/jit_validate.hpp"
#include "operators/jit_operator/operators/jit_write_tuples.hpp"
#include "operators/operator_scan_predicate.hpp"
#include "storage/storage_manager.hpp"
#include "types.hpp"

using namespace std::string_literals;  // NOLINT

namespace {

using namespace opossum;  // NOLINT

const std::unordered_map<PredicateCondition, JitExpressionType> predicate_condition_to_jit_expression_type = {
    {PredicateCondition::Equals, JitExpressionType::Equals},
    {PredicateCondition::NotEquals, JitExpressionType::NotEquals},
    {PredicateCondition::LessThan, JitExpressionType::LessThan},
    {PredicateCondition::LessThanEquals, JitExpressionType::LessThanEquals},
    {PredicateCondition::GreaterThan, JitExpressionType::GreaterThan},
    {PredicateCondition::GreaterThanEquals, JitExpressionType::GreaterThanEquals},
    {PredicateCondition::Between, JitExpressionType::Between},
    {PredicateCondition::Like, JitExpressionType::Like},
    {PredicateCondition::NotLike, JitExpressionType::NotLike},
    {PredicateCondition::IsNull, JitExpressionType::IsNull},
    {PredicateCondition::IsNotNull, JitExpressionType::IsNotNull},
    {PredicateCondition::In, JitExpressionType::In}};

const std::unordered_map<ArithmeticOperator, JitExpressionType> arithmetic_operator_to_jit_expression_type = {
    {ArithmeticOperator::Addition, JitExpressionType::Addition},
    {ArithmeticOperator::Subtraction, JitExpressionType::Subtraction},
    {ArithmeticOperator::Multiplication, JitExpressionType::Multiplication},
    {ArithmeticOperator::Division, JitExpressionType::Division},
    {ArithmeticOperator::Modulo, JitExpressionType::Modulo}};

const std::unordered_map<LogicalOperator, JitExpressionType> logical_operator_to_jit_expression = {
    {LogicalOperator::And, JitExpressionType::And}, {LogicalOperator::Or, JitExpressionType::Or}};

bool requires_computation(const std::shared_ptr<AbstractLQPNode>& node) {
  // do not count trivial projections without computations
  if (const auto projection_node = std::dynamic_pointer_cast<ProjectionNode>(node)) {
    for (const auto& expression : projection_node->column_expressions()) {
      if (expression->type != ExpressionType::LQPColumn) return true;
    }
    return false;
  }
  return true;
}

}  // namespace

namespace opossum {

std::shared_ptr<AbstractOperator> JitAwareLQPTranslator::translate_node(
    const std::shared_ptr<AbstractLQPNode>& node) const {
  // Jit operators materialize their output table and cannot be used in non-select queries
  if (node->type == LQPNodeType::Update || node->type == LQPNodeType::Delete) {
    return LQPTranslator{}.translate_node(node);
  }
  const auto jit_operator = _try_translate_sub_plan_to_jit_operators(node);
  return jit_operator ? jit_operator : LQPTranslator::translate_node(node);
}

std::shared_ptr<JitOperatorWrapper> JitAwareLQPTranslator::_try_translate_sub_plan_to_jit_operators(
    const std::shared_ptr<AbstractLQPNode>& node) const {
  auto jittable_node_count = size_t{0};

  auto input_nodes = std::unordered_set<std::shared_ptr<AbstractLQPNode>>{};

  bool use_validate = false;
  bool validate_after_filter = false;

  // Traverse query tree until a non-jittable nodes is found in each branch
  _visit(node, [&](auto& current_node) {
    const auto is_root_node = current_node == node;
    if (_node_is_jittable(current_node, is_root_node)) {
      use_validate |= current_node->type == LQPNodeType::Validate;
      validate_after_filter |= use_validate && current_node->type == LQPNodeType::Predicate;
      if (requires_computation(current_node)) ++jittable_node_count;
      return true;
    } else {
      input_nodes.insert(current_node);
      return false;
    }
  });

  // We use a really simple heuristic to decide when to introduce jittable operators:
  //   - If there is more than one input node, don't JIT
  //   - Always JIT AggregateNodes, as the JitAggregate is significantly faster than the Aggregate operator
  //   - Otherwise, JIT if there are two or more jittable nodes
  if (input_nodes.size() != 1 || jittable_node_count < 1) return nullptr;
  if (jittable_node_count == 1 && (node->type == LQPNodeType::Projection || node->type == LQPNodeType::Validate)) {
    return nullptr;
  }

  // limit can only be the root node
  const bool use_limit = node->type == LQPNodeType::Limit;
  std::shared_ptr<AbstractExpression> row_count_expression = nullptr;
  if (use_limit) {
    row_count_expression = std::static_pointer_cast<LimitNode>(node)->num_rows_expression();
  }

  // The input_node is not being integrated into the operator chain, but instead serves as the input to the JitOperators
  const auto input_node = *input_nodes.begin();

  const auto jit_operator = std::make_shared<JitOperatorWrapper>(translate_node(input_node));
  const auto read_tuples = std::make_shared<JitReadTuples>(use_validate, row_count_expression);
  jit_operator->add_jit_operator(read_tuples);

  // "filter_node". The root node of the subplan computed by a JitFilter.
  auto filter_node = node;
  while (filter_node != input_node && filter_node->type != LQPNodeType::Predicate &&
         filter_node->type != LQPNodeType::Union) {
    filter_node = filter_node->left_input();
  }

  if (use_validate && !validate_after_filter) jit_operator->add_jit_operator(std::make_shared<JitValidate>());

  // If we can reach the input node without encountering a UnionNode or PredicateNode,
  // there is no need to filter any tuples
  if (filter_node != input_node) {
    const auto boolean_expression = lqp_subplan_to_boolean_expression(filter_node);
    if (!boolean_expression) return nullptr;

    const auto jit_boolean_expression =
        _try_translate_expression_to_jit_expression(*boolean_expression, *read_tuples, input_node);
    if (!jit_boolean_expression) return nullptr;

    // make sure that the expression gets computed ...
    jit_operator->add_jit_operator(std::make_shared<JitCompute>(jit_boolean_expression));
    // and then filter on the resulting boolean.
    jit_operator->add_jit_operator(std::make_shared<JitFilter>(jit_boolean_expression->result()));
  }

  if (use_validate && validate_after_filter) jit_operator->add_jit_operator(std::make_shared<JitValidate>());

  if (node->type == LQPNodeType::Aggregate) {
    // Since aggregate nodes cause materialization, there is at most one JitAggregate operator in each operator chain
    // and it must be the last operator of the chain. The _node_is_jittable function takes care of this by rejecting
    // aggregate nodes that would be placed in the middle of an operator chain.
    const auto aggregate_node = std::static_pointer_cast<AggregateNode>(node);

    auto aggregate = std::make_shared<JitAggregate>();

    for (auto expression_idx = size_t{0}; expression_idx < aggregate_node->aggregate_expressions_begin_idx;
         ++expression_idx) {
      const auto& groupby_expression = aggregate_node->node_expressions[expression_idx];
      const auto jit_expression =
          _try_translate_expression_to_jit_expression(*groupby_expression, *read_tuples, input_node);
      if (!jit_expression) return nullptr;
      // Create a JitCompute operator for each computed groupby column ...
      if (jit_expression->expression_type() != JitExpressionType::Column) {
        jit_operator->add_jit_operator(std::make_shared<JitCompute>(jit_expression));
      }
      // ... and add the column to the JitAggregate operator.
      aggregate->add_groupby_column(groupby_expression->as_column_name(), jit_expression->result());
    }

    for (auto expression_idx = aggregate_node->aggregate_expressions_begin_idx;
         expression_idx < aggregate_node->node_expressions.size(); ++expression_idx) {
      const auto& expression = aggregate_node->node_expressions[expression_idx];
      const auto aggregate_expression = std::dynamic_pointer_cast<AggregateExpression>(expression);
      DebugAssert(aggregate_expression, "Expression is not a function.");

      const auto jit_expression =
          _try_translate_expression_to_jit_expression(*aggregate_expression->arguments[0], *read_tuples, input_node);
      if (!jit_expression) return nullptr;
      // Create a JitCompute operator for each aggregate expression on a computed value ...
      if (jit_expression->expression_type() != JitExpressionType::Column) {
        jit_operator->add_jit_operator(std::make_shared<JitCompute>(jit_expression));
      }
      // ... and add the aggregate expression to the JitAggregate operator.
      aggregate->add_aggregate_column(aggregate_expression->as_column_name(), jit_expression->result(),
                                      aggregate_expression->aggregate_function);
    }

    jit_operator->add_jit_operator(aggregate);
  } else {
    if (use_limit) jit_operator->add_jit_operator(std::make_shared<JitLimit>());

    // Add a compute operator for each computed output column (i.e., a column that is not from a stored table).
    auto write_table = std::make_shared<JitWriteTuples>();
    for (const auto& column_expression : node->column_expressions()) {
      const auto jit_expression =
          _try_translate_expression_to_jit_expression(*column_expression, *read_tuples, input_node);
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

std::shared_ptr<const JitExpression> JitAwareLQPTranslator::_try_translate_expression_to_jit_expression(
    const AbstractExpression& expression, JitReadTuples& jit_source,
    const std::shared_ptr<AbstractLQPNode>& input_node) const {
  const auto input_node_column_id = input_node->find_column_id(expression);
  if (input_node_column_id) {
    const auto tuple_value = jit_source.add_input_column(
        expression.data_type(), input_node->is_column_nullable(input_node->get_column_id(expression)),
        *input_node_column_id);
    return std::make_shared<JitExpression>(tuple_value);
  }

  std::shared_ptr<const JitExpression> left, right;
  switch (expression.type) {
    case ExpressionType::Value: {
      const auto* value_expression = dynamic_cast<const ValueExpression*>(&expression);
      const auto tuple_value = jit_source.add_literal_value(value_expression->value);
      return std::make_shared<JitExpression>(tuple_value);
    }

    case ExpressionType::LQPColumn:
      // Column SHOULD have been resolved by `find_column_id()` call above the switch
      Fail("Column doesn't exist in input_node");

    case ExpressionType::Predicate:
    case ExpressionType::Arithmetic:
    case ExpressionType::Logical: {
      std::vector<std::shared_ptr<const JitExpression>> jit_expression_arguments;
      for (const auto& argument : expression.arguments) {
        const auto jit_expression = _try_translate_expression_to_jit_expression(*argument, jit_source, input_node);
        if (!jit_expression) return nullptr;
        jit_expression_arguments.emplace_back(jit_expression);
      }

      const auto jit_expression_type = _expression_to_jit_expression_type(expression);

      if (jit_expression_arguments.size() == 1) {
        return std::make_shared<JitExpression>(jit_expression_arguments[0], jit_expression_type,
                                               jit_source.add_temporary_value());
      } else if (jit_expression_arguments.size() == 2) {
        // An expression can handle strings only exclusively
        if ((jit_expression_arguments[0]->result().data_type() == DataType::String) !=
            (jit_expression_arguments[1]->result().data_type() == DataType::String)) {
          return nullptr;
        }
        return std::make_shared<JitExpression>(jit_expression_arguments[0], jit_expression_type,
                                               jit_expression_arguments[1], jit_source.add_temporary_value());
      } else if (jit_expression_arguments.size() == 3) {
        DebugAssert(jit_expression_type == JitExpressionType::Between, "Only Between supported for 3 arguments");
        auto lower_bound_check =
            std::make_shared<JitExpression>(jit_expression_arguments[0], JitExpressionType::GreaterThanEquals,
                                            jit_expression_arguments[1], jit_source.add_temporary_value());
        auto upper_bound_check =
            std::make_shared<JitExpression>(jit_expression_arguments[0], JitExpressionType::LessThanEquals,
                                            jit_expression_arguments[2], jit_source.add_temporary_value());

        return std::make_shared<JitExpression>(lower_bound_check, JitExpressionType::And, upper_bound_check,
                                               jit_source.add_temporary_value());
      } else {
        Fail("Unexpected number of arguments, can't translate to JitExpression");
      }
    }

    default:
      return nullptr;
  }
}

bool JitAwareLQPTranslator::_node_is_jittable(const std::shared_ptr<AbstractLQPNode>& node,
                                              const bool is_root_node) const {
  if (node->type == LQPNodeType::Aggregate) {
    // We do not support the count distinct function yet and thus need to check all aggregate expressions.
    auto aggregate_node = std::static_pointer_cast<AggregateNode>(node);
    const auto& expressions = aggregate_node->node_expressions;
    auto has_unsupported_aggregate = std::any_of(
        expressions.begin() + aggregate_node->aggregate_expressions_begin_idx, expressions.end(), [](auto& expression) {
          const auto aggregate_expression = std::dynamic_pointer_cast<AggregateExpression>(expression);
          Assert(aggregate_expression, "Expected AggregateExpression");
          // Right now, the JIT doesn't support CountDistinct and Count(*) (which can be recognized by an empty
          // argument list)
          return aggregate_expression->aggregate_function == AggregateFunction::CountDistinct ||
                 aggregate_expression->arguments.empty();
        });
    // Aggregate must be the last node in the jittable operator pipeline. Hence, the the root node in the lpp.
    return is_root_node && !has_unsupported_aggregate;
  }

  if (node->type == LQPNodeType::Limit) {
    // Limit must be the last node in the jittable operator pipeline. Hence, the the root node in the lpp.
    return is_root_node;
  }

  if (auto predicate_node = std::dynamic_pointer_cast<PredicateNode>(node)) {
    return predicate_node->scan_type == ScanType::TableScan;
  }

  return node->type == LQPNodeType::Projection || node->type == LQPNodeType::Union ||
         node->type == LQPNodeType::Validate;
}

void JitAwareLQPTranslator::_visit(const std::shared_ptr<AbstractLQPNode>& node,
                                   const std::function<bool(const std::shared_ptr<AbstractLQPNode>&)>& func) const {
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

JitExpressionType JitAwareLQPTranslator::_expression_to_jit_expression_type(const AbstractExpression& expression) {
  switch (expression.type) {
    case ExpressionType::Arithmetic: {
      const auto* arithmetic_expression = dynamic_cast<const ArithmeticExpression*>(&expression);
      return arithmetic_operator_to_jit_expression_type.at(arithmetic_expression->arithmetic_operator);
    }

    case ExpressionType::Predicate: {
      const auto* predicate_expression = dynamic_cast<const AbstractPredicateExpression*>(&expression);
      return predicate_condition_to_jit_expression_type.at(predicate_expression->predicate_condition);
    }

    case ExpressionType::Logical: {
      const auto* logical_expression = dynamic_cast<const LogicalExpression*>(&expression);
      return logical_operator_to_jit_expression.at(logical_expression->logical_operator);
    }

    default:
      Fail("Expression "s + expression.as_column_name() + " is jit incompatible");
  }
}

}  // namespace opossum

#endif
