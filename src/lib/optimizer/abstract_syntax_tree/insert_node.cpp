#include "insert_node.hpp"

#include <algorithm>
#include <memory>
#include <sstream>
#include <string>
#include <vector>

#include "optimizer/expression.hpp"
#include "utils/assert.hpp"

namespace opossum {

InsertNode::InsertNode(const std::string table_name) : AbstractLogicalPlanNode(LQPNodeType::Insert), _table_name(table_name) {}

std::string InsertNode::description() const {
  std::ostringstream desc;

  desc << "[Insert] Into table '" << _table_name << "'";

  return desc.str();
}

const std::string& InsertNode::table_name() const { return _table_name; }

}  // namespace opossum
