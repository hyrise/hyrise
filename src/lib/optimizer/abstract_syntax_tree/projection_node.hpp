#pragma once

#include <sstream>
#include <vector>

#include "common.hpp"
#include "optimizer/abstract_syntax_tree/abstract_node.hpp"

namespace opossum {

class ProjectionNode : public AbstractNode {
 public:
  ProjectionNode(const std::vector<std::string>& column_names) : _column_names(column_names){};

  const std::string description() const override {
    std::ostringstream desc;

    desc << "Projection: ";

    for (auto& column : _column_names) {
      desc << " " << column;
    }

    return desc.str();
  }

 private:
  const std::vector<std::string> _column_names;
};

}  // namespace opossum
