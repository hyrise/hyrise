#include "projection_node.hpp"

#include <sstream>
#include <string>
#include <vector>

#include "common.hpp"

namespace opossum {

const std::string ProjectionNode::description() const {
  std::ostringstream desc;

  desc << "Projection: ";

  for (auto& column : _column_names) {
    desc << " " << column;
  }

  return desc.str();
}

const std::vector<std::string> ProjectionNode::output_columns() { return _column_names; }

}  // namespace opossum
