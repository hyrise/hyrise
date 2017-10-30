#pragma once

#include <memory>
#include <string>

#include "optimizer/abstract_syntax_tree/abstract_ast_node.hpp"

namespace opossum {

/**
 * Node type to represent deletion (more specifically, invalidation) in a table.
 */
class DeleteNode : public AbstractASTNode {
 public:
  explicit DeleteNode(const std::string& table_name);

  std::string description() const override;

  bool subtree_is_read_only() const override;

  const std::string& table_name() const;

  std::shared_ptr<AbstractASTNode> clone_subtree() const override;

 protected:
  const std::string _table_name;
};

}  // namespace opossum
