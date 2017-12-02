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
 * They are the leafs of every meaningful LQP tree.
 */
class StoredTableNode : public AbstractLQPNode {
 public:
  explicit StoredTableNode(const std::string& table_name);

  const std::string& table_name() const;

  std::string description() const override;
  const std::vector<ColumnID>& output_column_ids_to_input_column_ids() const override;
  const std::vector<std::string>& output_column_names() const override;

  std::shared_ptr<TableStatistics> derive_statistics_from(
      const std::shared_ptr<AbstractLQPNode>& left_child = nullptr,
      const std::shared_ptr<AbstractLQPNode>& right_child = nullptr) const override;

  bool knows_table(const std::string& table_name) const override;

  std::vector<ColumnID> get_output_column_ids_for_table(const std::string& table_name) const override;

  std::optional<ColumnID> find_column_id_by_named_column_reference(
      const NamedColumnReference& named_column_reference) const override;

  std::string get_verbose_column_name(ColumnID column_id) const override;

 protected:
  std::shared_ptr<AbstractLQPNode> _deep_copy_impl() const override;
  void _on_child_changed() override;

 private:
  const std::string _table_name;

  std::vector<std::string> _output_column_names;
};

}  // namespace opossum
