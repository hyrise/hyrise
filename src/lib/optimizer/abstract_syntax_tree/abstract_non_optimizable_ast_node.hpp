#pragma once

#include "optimizer/abstract_syntax_tree/abstract_ast_node.hpp"

namespace opossum {

/**
 * This class is a specialization used for nodes that are not intended to be optimized.
 * The current use case is nodes representing management commands, e.g. SHOW TABLES.
 */
class AbstractNonOptimizableASTNode : public AbstractASTNode {
 public:
  explicit AbstractNonOptimizableASTNode(ASTNodeType node_type);

  bool is_optimizable() const override;
};

}  // namespace opossum
