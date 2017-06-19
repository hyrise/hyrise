#pragma once

#include "common.hpp"
#include "optimizer/abstract_syntax_tree/abstract_node.hpp"

namespace opossum {

class TableNode : public AbstractNode {
 public:
  TableNode(const std::string table_name) : _table_name(table_name) {};

  const std::string description() const override {
    return "Table: " + _table_name;
  }

 private:
  const std::string _table_name;
};

}  // namespace opossum
