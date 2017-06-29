#pragma once

#include <string>
#include <vector>

#include "common.hpp"
#include "optimizer/abstract_syntax_tree/abstract_node.hpp"

namespace opossum {

class ProjectionNode : public AbstractNode {
 public:
  explicit ProjectionNode(const std::vector<std::string>& column_names);

  const std::string description() const override;

  const std::vector<std::string> output_columns() override;

 private:
  const std::vector<std::string> _column_names;
};

}  // namespace opossum
