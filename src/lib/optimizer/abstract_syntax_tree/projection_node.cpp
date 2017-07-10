#include "projection_node.hpp"

#include <sstream>
#include <string>
#include <vector>

#include "common.hpp"

namespace opossum {

ProjectionNode::ProjectionNode(const std::vector<std::string>& column_names)
    : AbstractAstNode(AstNodeType::Projection) {
  _output_columns = column_names;
}

std::string ProjectionNode::description() const {
  std::ostringstream desc;

  desc << "Projection: ";

  for (auto& column : _output_columns) {
    desc << " " << column;
  }

  return desc.str();
}

const std::vector<std::string>& ProjectionNode::output_columns() const { return _output_columns; }

}  // namespace opossum
