#pragma once

#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "all_parameter_variant.hpp"
#include "all_type_variant.hpp"
#include "common.hpp"
#include "types.hpp"

#include "optimizer/abstract_syntax_tree/abstract_ast_node.hpp"

namespace opossum {

struct ColumnID;

/**
 * This node type is used to represent any type of Join, including cross products.
 * The idea is that the optimizer is able to change the type of join if it sees fit.
 */
class JoinNode : public AbstractASTNode {
 public:
  JoinNode(optional<std::pair<ColumnID, ColumnID>> column_id, const ScanType scan_type, const JoinMode join_mode);

  optional<std::pair<ColumnID, ColumnID>> join_column_ids() const;
  ScanType scan_type() const;
  JoinMode join_mode() const;

  std::string description() const override;
  const std::vector<ColumnID> &output_column_ids() const override;
  const std::vector<std::string> &output_column_names() const override;
  bool manages_table(const std::string &table_name) const override;
  optional<ColumnID> find_column_id_for_column_identifier(const ColumnIdentifier &column_identifier) const override;

 protected:
  void _on_child_changed() override;

 private:
  optional<std::pair<ColumnID, ColumnID>> _join_column_ids;
  ScanType _scan_type;
  JoinMode _join_mode;

  std::vector<ColumnID> _output_column_ids;
  std::vector<std::string> _output_column_names;
};

}  // namespace opossum
