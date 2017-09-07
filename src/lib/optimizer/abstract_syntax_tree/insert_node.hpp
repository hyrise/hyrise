#pragma once

#include <memory>
#include <string>
#include <vector>

#include "common.hpp"
#include "optimizer/abstract_syntax_tree/abstract_ast_node.hpp"

namespace opossum {

/**
 * Node type to represent common projections, i.e. without any aggregate functionality.
 */
class InsertNode : public AbstractASTNode {
 public:
  explicit InsertNode(const std::string table_name /*, const std::shared_ptr<Table> values_to_insert */);

  std::string description() const override;

  const std::string table_name() const;

 protected:
  const std::string _table_name;
  // const std::shared_ptr<Table> _values_to_insert;
};

}  // namespace opossum
