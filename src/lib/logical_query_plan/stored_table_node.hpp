#pragma once

#include <optional>
#include <vector>

#include "abstract_lqp_node.hpp"
#include "expression/abstract_expression.hpp"
#include "lqp_column_reference.hpp"

namespace opossum {

class LQPColumnExpression;

class StoredTableNode : public EnableMakeForLQPNode<StoredTableNode>, public AbstractLQPNode {
 public:
  explicit StoredTableNode(const std::string& table_name);

  LQPColumnReference get_column(const std::string& name) const;

  void set_excluded_chunk_ids(const std::vector<ChunkID>& chunks);
  const std::vector<ChunkID>& excluded_chunk_ids() const;

  std::string description() const override;
  const std::vector<std::shared_ptr<AbstractExpression>>& column_expressions() const override;
  bool is_column_nullable(const ColumnID column_id) const override;
  std::shared_ptr<TableStatistics> derive_statistics_from(
      const std::shared_ptr<AbstractLQPNode>& left_input,
      const std::shared_ptr<AbstractLQPNode>& right_input) const override;

  const std::string table_name;

 protected:
  std::shared_ptr<AbstractLQPNode> _on_shallow_copy(LQPNodeMapping& node_mapping) const override;
  bool _on_shallow_equals(const AbstractLQPNode& rhs, const LQPNodeMapping& node_mapping) const override;

 private:
  mutable std::optional<std::vector<std::shared_ptr<AbstractExpression>>> _expressions;
  std::vector<ChunkID> _excluded_chunk_ids;
};

}  // namespace opossum
