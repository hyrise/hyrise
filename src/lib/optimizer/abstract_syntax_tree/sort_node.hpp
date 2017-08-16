#pragma once

#include <string>
#include <vector>

#include "common.hpp"
#include "optimizer/abstract_syntax_tree/abstract_ast_node.hpp"
#include "types.hpp"

namespace opossum {

/**
 * This node type represents sorting operations as defined in ORDER BY clauses.
 */
class SortNode : public AbstractASTNode {
 public:
  explicit SortNode(const ColumnID column_id, const bool asc);

  std::string description() const override;

  ColumnID column_id() const;
  bool ascending() const;

 private:
  const ColumnID _column_id;
  const bool _ascending;
};

}  // namespace opossum
