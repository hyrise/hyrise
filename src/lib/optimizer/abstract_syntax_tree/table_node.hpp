#pragma once

#include <memory>
#include <string>
#include <vector>

#include "common.hpp"
#include "optimizer/abstract_syntax_tree/abstract_node.hpp"

namespace opossum {

class TableNode : public AbstractNode {
 public:
  explicit TableNode(const std::string table_name);

  const std::string description() const override;

  const std::vector<std::string> output_columns() override;

  const std::string& table_name() const;

 private:
  std::shared_ptr<TableStatistics> create_statistics() const override;
  const std::string _table_name;
  std::vector<std::string> _column_names;
};

}  // namespace opossum
