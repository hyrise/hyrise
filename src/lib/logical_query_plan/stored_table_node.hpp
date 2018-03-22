#pragma once

#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "abstract_lqp_node.hpp"

namespace opossum {

struct ColumnID;
class TableStatistics;

/**
 * This node type represents a table stored by the table manager.
 * They are the leaves of every meaningful LQP tree.
 */
class StoredTableNode : public EnableMakeForLQPNode<StoredTableNode>, public AbstractLQPNode {
 public:
  explicit StoredTableNode(const std::string& table_name, const std::optional<std::string>& alias = std::nullopt);

  const std::string& table_name() const;

  std::string description() const override;
  std::shared_ptr<const AbstractLQPNode> find_table_name_origin(const std::string& table_name) const override;
  const std::vector<std::string>& output_column_names() const override;

  std::shared_ptr<TableStatistics> derive_statistics_from(
      const std::shared_ptr<AbstractLQPNode>& left_input = nullptr,
      const std::shared_ptr<AbstractLQPNode>& right_input = nullptr) const override;

  std::string get_verbose_column_name(ColumnID column_id) const override;

  void set_excluded_chunk_ids(const std::vector<ChunkID>& chunks);
  const std::vector<ChunkID>& excluded_chunk_ids() const;

  bool shallow_equals(const AbstractLQPNode& rhs) const override;

 protected:
  std::shared_ptr<AbstractLQPNode> _deep_copy_impl(
      const std::shared_ptr<AbstractLQPNode>& copied_left_input,
      const std::shared_ptr<AbstractLQPNode>& copied_right_input) const override;
  void _on_input_changed() override;
  std::optional<QualifiedColumnName> _resolve_local_table_name(
      const QualifiedColumnName& qualified_column_name) const override;

 private:
  const std::string _table_name;
  std::vector<ChunkID> _excluded_chunk_ids;

  std::vector<std::string> _output_column_names;
};

}  // namespace opossum
