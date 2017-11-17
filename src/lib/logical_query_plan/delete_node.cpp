#include "delete_node.hpp"

#include <algorithm>
#include <memory>
#include <sstream>
#include <string>

#include "utils/assert.hpp"

namespace opossum {

DeleteNode::DeleteNode(const std::string& table_name)
    : AbstractLQPNode(LQPNodeType::Delete), _table_name(table_name) {}

std::string DeleteNode::description() const {
  std::ostringstream desc;

  desc << "[Delete] Table: '" << _table_name << "'";

  return desc.str();
}

const std::string& DeleteNode::table_name() const { return _table_name; }

}  // namespace opossum
