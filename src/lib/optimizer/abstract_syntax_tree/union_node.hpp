#pragma once

#include <string>

#include "optimizer/abstract_syntax_tree/abstract_ast_node.hpp"
#include "types.hpp"

namespace opossum {

class UnionNode : public AbstractASTNode {
 public:
  explicit UnionNode(UnionMode union_mode);

  UnionMode union_mode() const;

  std::string description() const override;

  std::string get_verbose_column_name(ColumnID column_id) const override;

 private:
  UnionMode _union_mode;
};
}  // namespace opossum
