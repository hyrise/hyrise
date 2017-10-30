#include "insert_node.hpp"

#include <algorithm>
#include <memory>
#include <sstream>
#include <string>
#include <vector>

#include "optimizer/expression.hpp"
#include "utils/assert.hpp"

namespace opossum {

InsertNode::InsertNode(const std::string table_name) : AbstractASTNode(ASTNodeType::Insert), _table_name(table_name) {}

std::string InsertNode::description() const {
  std::ostringstream desc;

  desc << "[Insert] Into table '" << _table_name << "'";

  return desc.str();
}

bool InsertNode::subtree_is_read_only() const { return false; }

const std::string& InsertNode::table_name() const { return _table_name; }

std::shared_ptr<AbstractASTNode> InsertNode::clone_subtree() const {
  return _clone_without_subclass_members<decltype(*this)>();
}

}  // namespace opossum
