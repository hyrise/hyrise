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

class JoinNode : public AbstractASTNode {
 public:
  JoinNode(optional<std::pair<std::string, std::string>> column_names, const ScanType scan_type,
           const JoinMode join_mode, const std::string &prefix_left, const std::string &prefix_right);

  std::string description() const override;

  std::vector<std::string> output_column_names() const override;

  optional<std::pair<std::string, std::string>> join_column_names() const;
  ScanType scan_type() const;
  JoinMode join_mode() const;
  std::string prefix_left() const;
  std::string prefix_right() const;

 private:
  optional<std::pair<std::string, std::string>> _join_column_names;
  ScanType _scan_type;
  JoinMode _join_mode;
  std::string _prefix_left;
  std::string _prefix_right;
};

}  // namespace opossum
