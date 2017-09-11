#pragma once

#include <memory>
#include <string>
#include <vector>

#include "common.hpp"
#include "optimizer/abstract_syntax_tree/abstract_ast_node.hpp"

namespace opossum {

/**
 * Node type to represent insertion of rows into a table.
 */
class InsertNode : public AbstractASTNode {
 public:
  explicit InsertNode(const std::string table_name);

  std::string description() const override;

  const std::string &table_name() const;

 protected:
  const std::string _table_name;
};

}  // namespace opossum
