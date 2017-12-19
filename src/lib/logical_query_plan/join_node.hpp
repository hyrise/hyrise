#pragma once

#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "types.hpp"

#include "abstract_lqp_node.hpp"
#include "column_origin.hpp"

namespace opossum {

using JoinColumnOrigins = std::pair<ColumnOrigin, ColumnOrigin>;

/**
 * This node type is used to represent any type of Join, including cross products.
 * The idea is that the optimizer is able to decide on the physical join implementation.
 */
class JoinNode : public AbstractLQPNode {
 public:
  // Constructor for Natural and Cross Joins
  explicit JoinNode(const JoinMode join_mode);

  // Constructor for predicated Joins
  JoinNode(const JoinMode join_mode, const JoinColumnOrigins& join_column_origins, const ScanType scan_type);

  const std::optional<JoinColumnOrigins>& join_column_origins() const;
  const std::optional<ScanType>& scan_type() const;
  JoinMode join_mode() const;

  std::string description() const override;
  const std::vector<std::string>& output_column_names() const override;
  const std::vector<ColumnOrigin>& output_column_origins() const override;

  std::shared_ptr<TableStatistics> derive_statistics_from(
      const std::shared_ptr<AbstractLQPNode>& left_child,
      const std::shared_ptr<AbstractLQPNode>& right_child) const override;

  std::string get_verbose_column_name(ColumnID column_id) const override;

 protected:
  void _on_child_changed() override;

 private:
  JoinMode _join_mode;
  std::optional<JoinColumnOrigins> _join_column_origins;
  std::optional<ScanType> _scan_type;

  mutable std::optional<std::vector<std::string>> _output_column_names;

  void _update_output() const;
};

}  // namespace opossum
