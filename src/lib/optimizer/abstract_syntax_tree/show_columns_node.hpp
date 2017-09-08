#pragma once

#include <string>

#include "optimizer/abstract_syntax_tree/abstract_non_optimizable_ast_node.hpp"

namespace opossum {

/**
 * This node type represents the SHOW COLUMNS management command.
 */
class ShowColumnsNode : public AbstractNonOptimizableASTNode {
 public:
  explicit ShowColumnsNode(const std::string &table_name);

  std::string description() const override;

  const std::string &table_name() const;

 private:
  const std::string _table_name;
};

}  // namespace opossum
