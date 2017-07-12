#include "projection_node.hpp"

#include <sstream>
#include <string>
#include <vector>

#include "common.hpp"

namespace opossum {

ProjectionNode::ProjectionNode(const std::vector<std::string>& column_names)
    : AbstractASTNode(ASTNodeType::Projection) {
  _output_column_names = column_names;
}

std::string ProjectionNode::description() const {
  std::ostringstream desc;

  desc << "Projection: ";

  for (auto& column : _output_column_names) {
    desc << " " << column;
  }

  return desc.str();
}

const std::vector<std::string>& ProjectionNode::output_column_names() const { return _output_column_names; }

}  // namespace opossum
