#pragma once

#include <string>
#include <vector>

#include "common.hpp"
#include "optimizer/abstract_syntax_tree/abstract_ast_node.hpp"

namespace opossum {

class StoredTableNode : public AbstractASTNode {
 public:
  explicit StoredTableNode(const std::string &table_name);

  std::string description() const override;

  std::vector<std::string> output_column_names() const override;

  const std::string& table_name() const;

 private:
  const std::string _table_name;
};

}  // namespace opossum
