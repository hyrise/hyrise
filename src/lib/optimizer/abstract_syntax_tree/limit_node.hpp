#pragma once

#include <memory>
#include <string>

#include "optimizer/abstract_syntax_tree/abstract_ast_node.hpp"

namespace opossum {

/**
 * This node type represents limiting a result to a certain number of rows (LIMIT operator).
 */
class LimitNode : public AbstractASTNode {
 public:
  explicit LimitNode(const size_t num_rows);

  std::string description() const override;

  size_t num_rows() const;

  std::shared_ptr<AbstractASTNode> clone_subtree() const override;

 private:
  const size_t _num_rows;
};

}  // namespace opossum
