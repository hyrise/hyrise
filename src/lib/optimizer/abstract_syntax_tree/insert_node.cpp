#include "insert_node.hpp"

#include <algorithm>
#include <memory>
#include <sstream>
#include <string>
#include <vector>

#include "common.hpp"
#include "optimizer/expression/expression_node.hpp"
#include "utils/assert.hpp"

namespace opossum {

InsertNode::InsertNode(const std::string table_name, std::vector<std::string> insert_columns)
    : AbstractASTNode(ASTNodeType::Insert), _table_name(table_name), _insert_columns(insert_columns) {}

std::string InsertNode::description() const {
  std::ostringstream desc;

  desc << "Insert into " << _table_name;

  return desc.str();
}

const std::string &InsertNode::table_name() const { return _table_name; }

const std::vector<std::string> &InsertNode::insert_columns() const { return _insert_columns; }

}  // namespace opossum
