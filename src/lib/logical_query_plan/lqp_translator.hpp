#pragma once

#include <memory>
#include <unordered_map>

#include "abstract_lqp_node.hpp"
#include "all_type_variant.hpp"
#include "operators/abstract_operator.hpp"

namespace opossum {

class AbstractOperator;
class TransactionContext;
class AbstractExpression;
class PredicateNode;
struct OperatorScanPredicate;
struct OperatorJoinPredicate;

/**
 * Translates an LQP (Logical Query Plan), represented by its root node, into an Operator tree for the execution
 * engine, which in return is represented by its root Operator.
 */
class LQPTranslator {
 public:
  virtual ~LQPTranslator() = default;

  virtual std::shared_ptr<AbstractOperator> translate_node(const std::shared_ptr<AbstractLQPNode>& node) const;

 private:
  std::shared_ptr<AbstractOperator> _translate_by_node_type(LQPNodeType type,
                                                            const std::shared_ptr<AbstractLQPNode>& node) const;

  std::shared_ptr<AbstractOperator> _translate_stored_table_node(const std::shared_ptr<AbstractLQPNode>& node) const;
  std::shared_ptr<AbstractOperator> _translate_predicate_node(const std::shared_ptr<AbstractLQPNode>& node) const;
  std::shared_ptr<AbstractOperator> _translate_predicate_node_to_index_scan(
      const std::shared_ptr<PredicateNode>& node, const std::shared_ptr<AbstractOperator>& input_operator) const;
  std::shared_ptr<AbstractOperator> _translate_predicate_node_to_table_scan(
      const OperatorScanPredicate& operator_scan_predicate,
      const std::shared_ptr<AbstractOperator>& input_operator) const;
  std::shared_ptr<AbstractOperator> _translate_alias_node(const std::shared_ptr<AbstractLQPNode>& node) const;
  std::shared_ptr<AbstractOperator> _translate_projection_node(const std::shared_ptr<AbstractLQPNode>& node) const;
  std::shared_ptr<AbstractOperator> _translate_sort_node(const std::shared_ptr<AbstractLQPNode>& node) const;
  std::shared_ptr<AbstractOperator> _translate_join_node(const std::shared_ptr<AbstractLQPNode>& node) const;
  std::shared_ptr<AbstractOperator> _translate_aggregate_node(const std::shared_ptr<AbstractLQPNode>& node) const;
  std::shared_ptr<AbstractOperator> _translate_limit_node(const std::shared_ptr<AbstractLQPNode>& node) const;
  std::shared_ptr<AbstractOperator> _translate_insert_node(const std::shared_ptr<AbstractLQPNode>& node) const;
  std::shared_ptr<AbstractOperator> _translate_delete_node(const std::shared_ptr<AbstractLQPNode>& node) const;
  std::shared_ptr<AbstractOperator> _translate_dummy_table_node(const std::shared_ptr<AbstractLQPNode>& node) const;
  std::shared_ptr<AbstractOperator> _translate_update_node(const std::shared_ptr<AbstractLQPNode>& node) const;
  std::shared_ptr<AbstractOperator> _translate_union_node(const std::shared_ptr<AbstractLQPNode>& node) const;
  std::shared_ptr<AbstractOperator> _translate_validate_node(const std::shared_ptr<AbstractLQPNode>& node) const;

  // Maintenance operators
  std::shared_ptr<AbstractOperator> _translate_show_tables_node(const std::shared_ptr<AbstractLQPNode>& node) const;
  std::shared_ptr<AbstractOperator> _translate_show_columns_node(const std::shared_ptr<AbstractLQPNode>& node) const;

  // Translate LQP- to PQPExpressions
  std::vector<std::shared_ptr<AbstractExpression>> _translate_expressions(
      const std::vector<std::shared_ptr<AbstractExpression>>& lqp_expressions,
      const std::shared_ptr<AbstractLQPNode>& node) const;

  std::shared_ptr<AbstractOperator> _translate_create_view_node(const std::shared_ptr<AbstractLQPNode>& node) const;
  std::shared_ptr<AbstractOperator> _translate_drop_view_node(const std::shared_ptr<AbstractLQPNode>& node) const;

  static std::shared_ptr<AbstractOperator> _translate_binary_predicate_to_table_scan(
      const AbstractLQPNode& input_node, const std::shared_ptr<AbstractOperator>& input_operator,
      const AbstractExpression& left_operand, const PredicateCondition predicate_condition,
      const AbstractExpression& right_operand);

  static std::shared_ptr<AbstractOperator> _translate_unary_predicate_to_table_scan(
      const AbstractLQPNode& input_node, const std::shared_ptr<AbstractOperator>& input_operator,
      const AbstractExpression& operand, const PredicateCondition predicate_condition);

  static AllParameterVariant _translate_to_all_parameter_variant(const AbstractLQPNode& input_node,
                                                                 const AbstractExpression& expression);

  // Cache operator subtrees by LQP node to avoid executing operators below a diamond shape multiple times
  mutable std::unordered_map<std::shared_ptr<const AbstractLQPNode>, std::shared_ptr<AbstractOperator>>
      _operator_by_lqp_node;
};

}  // namespace opossum
