#pragma once

#include <string>
#include <vector>

#include "common.hpp"
#include "optimizer/abstract_syntax_tree/abstract_ast_node.hpp"

namespace opossum {

class TableNode : public AbstractAstNode {
 public:
  explicit TableNode(const std::string table_name);

  std::string description() const override;

  const std::vector<std::string> & output_columns() const override;

  const std::string& table_name() const;

 private:
  const std::string _table_name;
  std::vector<std::string> _column_names;
};

}  // namespace opossum
