#pragma once

#include <memory>
#include <unordered_map>

#include "abstract_lqp_node.hpp"
#include "all_type_variant.hpp"
#include "operators/abstract_operator.hpp"

namespace opossum {

class AbstractOperator;
class TransactionContext;

/**
 * Translates an LQP (Logical Query Plan), represented by its root node, into an Operator tree for the execution
 * engine, which in return is represented by its root Operator.
 */
class LQPTranslator final : private Noncopyable {
 public:
  std::shared_ptr<AbstractOperator> translate_node(const std::shared_ptr<AbstractLQPNode>& node) const;

 private:
  std::shared_ptr<AbstractOperator> _translate_by_node_type(LQPNodeType type,
                                                            const std::shared_ptr<AbstractLQPNode>& node) const;

  // SQL operators
  std::shared_ptr<AbstractOperator> _translate_stored_table_node(const std::shared_ptr<AbstractLQPNode>& node) const;
  std::shared_ptr<AbstractOperator> _translate_predicate_node(const std::shared_ptr<AbstractLQPNode>& node) const;
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
  std::shared_ptr<AbstractOperator> _translate_create_view_node(const std::shared_ptr<AbstractLQPNode>& node) const;
  std::shared_ptr<AbstractOperator> _translate_drop_view_node(const std::shared_ptr<AbstractLQPNode>& node) const;

  // Cache operator subtrees by LQP node to avoid executing operators below a diamond shape multiple times
  mutable std::unordered_map<std::shared_ptr<const AbstractLQPNode>, std::shared_ptr<AbstractOperator>>
      _operator_by_lqp_node;
};

}  // namespace opossum
