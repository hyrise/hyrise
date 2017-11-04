#pragma once

#include <string>

#include "optimizer/abstract_syntax_tree/abstract_ast_node.hpp"

namespace opossum {

/**
 * This node type represents limiting a result to a certain number of rows (LIMIT operator).
 */
class LimitNode : public AbstractASTNode {
 public:
  explicit LimitNode(const size_t num_rows);

  std::string description(DescriptionMode mode) const override;

  size_t num_rows() const;

 private:
  const size_t _num_rows;
};

}  // namespace opossum
