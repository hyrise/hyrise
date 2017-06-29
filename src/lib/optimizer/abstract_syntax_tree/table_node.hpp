#pragma once

#include <string>
#include <vector>

#include "common.hpp"
#include "optimizer/abstract_syntax_tree/abstract_node.hpp"

namespace opossum {

class TableNode : public AbstractNode {
 public:
  explicit TableNode(const std::string table_name) : _table_name(table_name) { _type = TableNodeType; }

  const std::string description() const override;

  const std::vector<std::string> output_columns() override;

 protected:
  virtual std::shared_ptr<TableStatistics> create_statistics() const;

 private:
  const std::string _table_name;
  std::vector<std::string> _column_names;
};

}  // namespace opossum
