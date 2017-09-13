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

/**
 * This node type is used to represent any type of Join, including cross products.
 * The idea is that the optimizer is able to decide on the physical join implementation.
 */
class JoinNode : public AbstractASTNode {
 public:
  explicit JoinNode(const JoinMode join_mode);

  JoinNode(const JoinMode join_mode, const std::pair<ColumnID, ColumnID> &join_column_ids, const ScanType scan_type);

  const optional<std::pair<ColumnID, ColumnID>> &join_column_ids() const;
  const optional<ScanType> &scan_type() const;
  JoinMode join_mode() const;

  std::string description() const override;
  const std::vector<ColumnID> &output_column_id_to_input_column_id() const override;
  const std::vector<std::string> &output_column_names() const override;

  bool knows_table(const std::string &table_name) const override;
  std::vector<ColumnID> get_output_column_ids_for_table(const std::string &table_name) const override;

  optional<ColumnID> find_column_id_by_named_column_reference(
      const NamedColumnReference &named_column_reference) const override;

 protected:
  void _on_child_changed() override;

 private:
  JoinMode _join_mode;
  optional<std::pair<ColumnID, ColumnID>> _join_column_ids;
  optional<ScanType> _scan_type;

  std::vector<ColumnID> _output_column_id_to_input_column_id;
  std::vector<std::string> _output_column_names;
};

}  // namespace opossum
