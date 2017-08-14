#include "projection_node.hpp"

#include <sstream>
#include <string>
#include <vector>

#include "common.hpp"
#include "types.hpp"

namespace opossum {

ProjectionNode::ProjectionNode(const std::vector<ColumnID>& column_ids)
    : AbstractASTNode(ASTNodeType::Projection) {
  _output_column_ids = column_ids;
}

std::string ProjectionNode::description() const {
  std::ostringstream desc;

  desc << "Projection: ";

  for (auto& column : _output_column_ids) {
//    TODO(Sven): fix
    desc << " " << column;
  }

  return desc.str();
}

std::vector<ColumnID> ProjectionNode::output_column_ids() const { return _output_column_ids; }

}  // namespace opossum
