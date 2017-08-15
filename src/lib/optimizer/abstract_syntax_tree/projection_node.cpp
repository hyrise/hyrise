#include "projection_node.hpp"

#include <sstream>
#include <string>
#include <utils/assert.hpp>
#include <vector>

#include "common.hpp"
#include "types.hpp"

namespace opossum {

ProjectionNode::ProjectionNode(const std::vector<ColumnID>& column_ids,
                               const std::vector<std::string>& output_column_names)
    : AbstractASTNode(ASTNodeType::Projection) {
  _output_column_ids = column_ids;
  _output_column_names = output_column_names;
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

const std::vector<ColumnID> ProjectionNode::output_column_ids() const { return _output_column_ids; }
const std::vector<std::string> ProjectionNode::output_column_names() const { return _output_column_names; }

const optional<ColumnID> ProjectionNode::find_column_id_for_column_identifier(
    ColumnIdentifier& column_identifier) const {
  optional<ColumnID> found = nullopt;
  for (size_t i = 0; i < _output_column_names.size(); i++) {
    const auto& name = _output_column_names[i];
    if (column_identifier.column_name == name) {
      if (!found) {
        found = ColumnID{i};
      } else {
        Fail("Column name " + column_identifier.column_name + " is ambiguous.");
      }
    }
  }

  return found;
}

}  // namespace opossum
