#pragma once

#include <memory>
#include <unordered_map>

#include "abstract_lqp_node.hpp"
#include "all_type_variant.hpp"
#include "operators/abstract_operator.hpp"
#include "predicate_node.hpp"

namespace opossum {

class AbstractOperator;
class TransactionContext;
class LQPExpression;
class PQPExpression;

/**
 * Translates an LQP (Logical Query Plan), represented by its root node, into an Operator tree for the execution
 * engine, which in return is represented by its root Operator.
 */
class LQPTranslator final : private Noncopyable {
 public:
  AbstractOperatorSPtr translate_node(const AbstractLQPNodeSPtr& node) const;

 private:
  AbstractOperatorSPtr _translate_by_node_type(LQPNodeType type,
                                                            const AbstractLQPNodeSPtr& node) const;

  // SQL operators
  AbstractOperatorSPtr _translate_stored_table_node(const AbstractLQPNodeSPtr& node) const;
  AbstractOperatorSPtr _translate_predicate_node(const AbstractLQPNodeSPtr& node) const;
  AbstractOperatorSPtr _translate_predicate_node_to_index_scan(
      const PredicateNodeSPtr& node, const AllParameterVariant& value, const ColumnID column_id,
      const AbstractOperatorSPtr input_operator) const;
  AbstractOperatorSPtr _translate_projection_node(const AbstractLQPNodeSPtr& node) const;
  AbstractOperatorSPtr _translate_sort_node(const AbstractLQPNodeSPtr& node) const;
  AbstractOperatorSPtr _translate_join_node(const AbstractLQPNodeSPtr& node) const;
  AbstractOperatorSPtr _translate_aggregate_node(const AbstractLQPNodeSPtr& node) const;
  AbstractOperatorSPtr _translate_limit_node(const AbstractLQPNodeSPtr& node) const;
  AbstractOperatorSPtr _translate_insert_node(const AbstractLQPNodeSPtr& node) const;
  AbstractOperatorSPtr _translate_delete_node(const AbstractLQPNodeSPtr& node) const;
  AbstractOperatorSPtr _translate_dummy_table_node(const AbstractLQPNodeSPtr& node) const;
  AbstractOperatorSPtr _translate_update_node(const AbstractLQPNodeSPtr& node) const;
  AbstractOperatorSPtr _translate_union_node(const AbstractLQPNodeSPtr& node) const;
  AbstractOperatorSPtr _translate_validate_node(const AbstractLQPNodeSPtr& node) const;

  // Maintenance operators
  AbstractOperatorSPtr _translate_show_tables_node(const AbstractLQPNodeSPtr& node) const;
  AbstractOperatorSPtr _translate_show_columns_node(const AbstractLQPNodeSPtr& node) const;

  // Translate LQP- to PQPExpressions
  std::vector<PQPExpressionSPtr> _translate_expressions(
      const std::vector<LQPExpressionSPtr>& lqp_expressions,
      const AbstractLQPNodeSPtr& node) const;

  AbstractOperatorSPtr _translate_create_view_node(const AbstractLQPNodeSPtr& node) const;
  AbstractOperatorSPtr _translate_drop_view_node(const AbstractLQPNodeSPtr& node) const;

  // Cache operator subtrees by LQP node to avoid executing operators below a diamond shape multiple times
  mutable std::unordered_map<AbstractLQPNodeCSPtr, AbstractOperatorSPtr>
      _operator_by_lqp_node;
};

}  // namespace opossum
