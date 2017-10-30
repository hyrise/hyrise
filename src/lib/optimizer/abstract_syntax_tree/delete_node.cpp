#include "delete_node.hpp"

#include <algorithm>
#include <memory>
#include <sstream>
#include <string>

#include "utils/assert.hpp"

namespace opossum {

DeleteNode::DeleteNode(const std::string& table_name) : AbstractASTNode(ASTNodeType::Delete), _table_name(table_name) {}

std::string DeleteNode::description() const {
  std::ostringstream desc;

  desc << "[Delete] Table: '" << _table_name << "'";

  return desc.str();
}

bool DeleteNode::subtree_is_read_only() const { return false; }

const std::string& DeleteNode::table_name() const { return _table_name; }

std::shared_ptr<AbstractASTNode> DeleteNode::clone_subtree() const {
  return _clone_without_subclass_members<decltype(*this)>();
}

}  // namespace opossum
