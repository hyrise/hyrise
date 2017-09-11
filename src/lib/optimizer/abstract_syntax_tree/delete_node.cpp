#include "delete_node.hpp"

#include <algorithm>
#include <memory>
#include <sstream>
#include <string>

#include "common.hpp"
#include "optimizer/expression.hpp"
#include "utils/assert.hpp"

namespace opossum {

DeleteNode::DeleteNode(const std::string &table_name) : AbstractASTNode(ASTNodeType::Delete), _table_name(table_name) {}

std::string DeleteNode::description() const {
  std::ostringstream desc;

  desc << "Delete from " << _table_name;

  return desc.str();
}

const std::string &DeleteNode::table_name() const { return _table_name; }

}  // namespace opossum
