#include "projection_node.hpp"

#include <sstream>
#include <string>
#include <vector>
#include <utils/assert.hpp>

#include "common.hpp"
#include "types.hpp"

namespace opossum {

ProjectionNode::ProjectionNode(const std::vector<ColumnID>& column_ids, const std::vector<std::string>& output_column_names)
  : AbstractASTNode(ASTNodeType::Projection), _output_column_names(output_column_names) {
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
std::vector<std::string> ProjectionNode::output_column_names() const { return _output_column_names; }

bool ProjectionNode::find_column_id_for_column_name(std::string & column_name, ColumnID &column_id) {
  std::vector<ColumnID> matches;
  for (size_t i = 0; i < _output_column_names.size(); i++) {
    const auto &name = _output_column_names[i];
    if (column_name == name) {
      matches.emplace_back(ColumnID{i});
    }
  }

  if (matches.size() != 1) {
    Fail("Either did not find column name or column name is ambiguous.");
  }

  column_id = matches[0];
  return true;
}

}  // namespace opossum
