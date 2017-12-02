#pragma once

#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "types.hpp"

#include "abstract_lqp_node.hpp"

namespace opossum {

/**
 * This node type is used to represent any type of Join, including cross products.
 * The idea is that the optimizer is able to decide on the physical join implementation.
 */
class JoinNode : public AbstractLQPNode {
 public:
  explicit JoinNode(const JoinMode join_mode);

  JoinNode(const JoinMode join_mode, const std::pair<ColumnID, ColumnID>& join_column_ids, const ScanType scan_type);

  const std::optional<std::pair<ColumnID, ColumnID>>& join_column_ids() const;
  const std::optional<ScanType>& scan_type() const;
  JoinMode join_mode() const;

  std::string description() const override;
  const std::vector<ColumnID>& output_column_ids_to_input_column_ids() const override;
  const std::vector<std::string>& output_column_names() const override;

  std::shared_ptr<TableStatistics> derive_statistics_from(
      const std::shared_ptr<AbstractLQPNode>& left_child,
      const std::shared_ptr<AbstractLQPNode>& right_child) const override;

  bool knows_table(const std::string& table_name) const override;
  std::vector<ColumnID> get_output_column_ids_for_table(const std::string& table_name) const override;

  std::optional<ColumnID> find_column_id_by_named_column_reference(
      const NamedColumnReference& named_column_reference) const override;

  std::string get_verbose_column_name(ColumnID column_id) const override;

 protected:
  void _on_child_changed() override;
  std::shared_ptr<AbstractLQPNode> _deep_copy_impl() const override;

 private:
  JoinMode _join_mode;
  std::optional<std::pair<ColumnID, ColumnID>> _join_column_ids;
  std::optional<ScanType> _scan_type;

  mutable std::optional<std::vector<std::string>> _output_column_names;

  void _update_output() const;
};

}  // namespace opossum
