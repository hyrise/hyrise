#pragma once

#include <string>
#include <vector>

#include "common.hpp"
#include "optimizer/abstract_syntax_tree/abstract_ast_node.hpp"

namespace opossum {

class SortNode : public AbstractASTNode {
 public:
  explicit SortNode(const std::string column_name, const bool asc);

  std::string description() const override;

  std::string column_name() const;
  bool ascending() const;

 private:
  const std::string _column_name;
  const bool _ascending;
};

}  // namespace opossum
