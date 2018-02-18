#include "jit_aware_lqp_translator.hpp"

#include <boost/range/combine.hpp>

#include <queue>
#include <unordered_set>

#include "constant_mappings.hpp"
#include "operators/jit_operator/operators/jit_filter.hpp"
#include "operators/jit_operator/operators/jit_read_table.hpp"
#include "operators/jit_operator/operators/jit_save_table.hpp"
#include "projection_node.hpp"
#include "storage/storage_manager.hpp"
#include "stored_table_node.hpp"

namespace opossum {

std::shared_ptr<AbstractOperator> JitAwareLQPTranslator::translate_node(
    const std::shared_ptr<AbstractLQPNode>& node) const {
  uint32_t num_jittable_nodes{0};
  std::unordered_set<std::shared_ptr<AbstractLQPNode>> input_nodes;

  _breadth_first_search(node, [&](auto& current_node) {
    switch (current_node->type()) {
      case LQPNodeType::Predicate:
        if (std::dynamic_pointer_cast<PredicateNode>(current_node)->scan_type() == ScanType::TableScan) {
          ++num_jittable_nodes;
          return true;
        } else {
          return false;
        }
      case LQPNodeType::Projection:
      case LQPNodeType::Union:
        ++num_jittable_nodes;
        return true;
      default:
        input_nodes.insert(current_node);
        return false;
    }
  });

  // It does not make sense to create a JitOperator for fewer than 2 LQP nodes
  if (num_jittable_nodes < 2 || input_nodes.size() != 1) {
    return LQPTranslator::translate_node(node);
  }
  const auto input_node = *input_nodes.begin();

  auto jit_operator = std::make_shared<JitOperator>(translate_node(input_node));
  auto get_table = std::make_shared<JitReadTable>();
  jit_operator->add_jit_operator(get_table);

  auto filter_node = node;
  while (filter_node != input_node && filter_node->type() != LQPNodeType::Predicate &&
         filter_node->type() != LQPNodeType::Union) {
    filter_node = filter_node->left_child();
  }

  // If we can reach the input node without encountering a UnionNode or PredicateNode, there is no need to filter at all
  if (filter_node != input_node) {
    const auto expression = _translate_to_jit_expression(filter_node, *get_table);
    jit_operator->add_jit_operator(std::make_shared<JitCompute>(expression));
    jit_operator->add_jit_operator(std::make_shared<JitFilter>(expression->result()));
  }

  // Identify the top-most projection node to determin the output columns
  auto top_projection = node;
  while (top_projection != input_node && top_projection->type() != LQPNodeType::Projection) {
    top_projection = top_projection->left_child();
  }

  // Add a compute operator for each output column that computes the column.
  auto save_table = std::make_shared<JitSaveTable>();
  for (const auto& output_column :
       boost::combine(top_projection->output_column_names(), top_projection->output_column_references())) {
    const auto expression = _translate_to_jit_expression(output_column.get<1>(), *get_table);
    if (expression->expression_type() != ExpressionType::Column) {
      jit_operator->add_jit_operator(std::make_shared<JitCompute>(expression));
    }
    save_table->add_output_column(output_column.get<0>(), expression->result());
  }
  jit_operator->add_jit_operator(save_table);
  return jit_operator;
}

JitExpression::Ptr JitAwareLQPTranslator::_translate_to_jit_expression(const std::shared_ptr<AbstractLQPNode>& node,
                                                                       JitReadTable& jit_source) const {
  JitExpression::Ptr left, right;
  switch (node->type()) {
    case LQPNodeType::Predicate:
      if (_has_another_condition(node->left_child())) {
        left = _translate_to_jit_expression(std::dynamic_pointer_cast<PredicateNode>(node), jit_source);
        right = _translate_to_jit_expression(node->left_child(), jit_source);
        return std::make_shared<JitExpression>(left, ExpressionType::And, right, jit_source.add_temorary_value());
      } else {
        return _translate_to_jit_expression(std::dynamic_pointer_cast<PredicateNode>(node), jit_source);
      }

    case LQPNodeType::Union:
      left = _translate_to_jit_expression(node->left_child(), jit_source);
      right = _translate_to_jit_expression(node->right_child(), jit_source);
      return std::make_shared<JitExpression>(left, ExpressionType::Or, right, jit_source.add_temorary_value());

    case LQPNodeType::Projection:
      return _translate_to_jit_expression(node->left_child(), jit_source);

    default:
      Fail("unexpected node type");
  }
}

JitExpression::Ptr JitAwareLQPTranslator::_translate_to_jit_expression(const std::shared_ptr<PredicateNode>& node,
                                                                       JitReadTable& jit_source) const {
  auto condition = predicate_condition_to_expression_type.at(node->predicate_condition());
  auto left = _translate_to_jit_expression(node->column_reference(), jit_source);
  JitExpression::Ptr right;

  switch (condition) {
    case ExpressionType::Equals:
    case ExpressionType::NotEquals:
    case ExpressionType::LessThan:
    case ExpressionType::LessThanEquals:
    case ExpressionType::GreaterThan:
    case ExpressionType::GreaterThanEquals:
    case ExpressionType::Like:
    case ExpressionType::NotLike:
      right = _translate_to_jit_expression(node->value(), jit_source);
      return std::make_shared<JitExpression>(left, condition, right, jit_source.add_temorary_value());
    case ExpressionType::Between:
      Fail("not supported yet");
    // TODO(johannes)
    case ExpressionType::IsNull:
    case ExpressionType::IsNotNull:
      return std::make_shared<JitExpression>(left, condition, jit_source.add_temorary_value());
    default:
      Fail("invalid expression type");
  }
}

JitExpression::Ptr JitAwareLQPTranslator::_translate_to_jit_expression(const LQPExpression& lqp_expression,
                                                                       JitReadTable& jit_source) const {
  JitExpression::Ptr left, right;
  switch (lqp_expression.type()) {
    case ExpressionType::Literal:
      return _translate_to_jit_expression(lqp_expression.value(), jit_source);

    case ExpressionType::Column:
      return _translate_to_jit_expression(lqp_expression.column_reference(), jit_source);

    // binary operators
    case ExpressionType::Addition:
    case ExpressionType::Subtraction:
    case ExpressionType::Multiplication:
    case ExpressionType::Division:
    case ExpressionType::Modulo:
    case ExpressionType::Power:
    case ExpressionType::Equals:
    case ExpressionType::NotEquals:
    case ExpressionType::LessThan:
    case ExpressionType::LessThanEquals:
    case ExpressionType::GreaterThan:
    case ExpressionType::GreaterThanEquals:
    case ExpressionType::Like:
    case ExpressionType::NotLike:
    case ExpressionType::And:
    case ExpressionType::Or:
      left = _translate_to_jit_expression(*lqp_expression.left_child(), jit_source);
      right = _translate_to_jit_expression(*lqp_expression.right_child(), jit_source);
      return std::make_shared<JitExpression>(left, lqp_expression.type(), right, jit_source.add_temorary_value());

    // unary operators
    case ExpressionType::Not:
    case ExpressionType::IsNull:
    case ExpressionType::IsNotNull:
      left = _translate_to_jit_expression(*lqp_expression.left_child(), jit_source);
      return std::make_shared<JitExpression>(left, lqp_expression.type(), jit_source.add_temorary_value());

    default:
      Fail("unexpected expression type");
  }
}

JitExpression::Ptr JitAwareLQPTranslator::_translate_to_jit_expression(const LQPColumnReference& lqp_column_reference,
                                                                       JitReadTable& jit_source) const {
  if (const auto projection_node =
          std::dynamic_pointer_cast<const ProjectionNode>(lqp_column_reference.original_node())) {
    const auto lqp_expression = projection_node->column_expressions()[lqp_column_reference.original_column_id()];
    return _translate_to_jit_expression(*lqp_expression, jit_source);
  } else if (const auto stored_table_node =
                 std::dynamic_pointer_cast<const StoredTableNode>(lqp_column_reference.original_node())) {
    const auto column_id = lqp_column_reference.original_column_id();
    const auto table_name = stored_table_node->table_name();
    const auto table = StorageManager::get().get_table(table_name);
    const auto tuple_value = jit_source.add_input_column(*table, column_id);
    return std::make_shared<JitExpression>(tuple_value);
  } else {
    Fail("unexpected node type");
  }
}

JitExpression::Ptr JitAwareLQPTranslator::_translate_to_jit_expression(const AllParameterVariant& value,
                                                                       JitReadTable& jit_source) const {
  if (is_lqp_column_reference(value)) {
    return _translate_to_jit_expression(boost::get<LQPColumnReference>(value), jit_source);
  } else if (is_variant(value)) {
    const auto variant = boost::get<AllTypeVariant>(value);
    const auto tuple_value = jit_source.add_literal_value(variant);
    return std::make_shared<JitExpression>(tuple_value);
  } else {
    Fail("unexpected parameter type");
  }
}

bool JitAwareLQPTranslator::_has_another_condition(const std::shared_ptr<AbstractLQPNode>& node) const {
  auto current_node = node;
  while (current_node->type() == LQPNodeType::Projection) {
    current_node = current_node->left_child();
  }
  return current_node->type() == LQPNodeType::Predicate || current_node->type() == LQPNodeType::Union;
}

void JitAwareLQPTranslator::_breadth_first_search(
    const std::shared_ptr<AbstractLQPNode>& node,
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
      queue.push(current_node->left_child());
      queue.push(current_node->right_child());
    }
  }
}

}  // namespace opossum
