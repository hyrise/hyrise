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

  std::string description() const override;

  const std::vector<ColumnID> output_column_ids() const override;
  const optional<ColumnID> find_column_id_for_column_identifier(ColumnIdentifier &column_identifier) const override;
  const bool manages_table(const std::string &table_name) const override;

  optional<std::pair<ColumnID, ColumnID>> join_column_ids() const;
  ScanType scan_type() const;
  JoinMode join_mode() const;

 private:
  optional<std::pair<ColumnID, ColumnID>> _join_column_ids;
  ScanType _scan_type;
  JoinMode _join_mode;
};

}  // namespace opossum
