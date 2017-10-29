#include "update_node.hpp"

#include <memory>
#include <sstream>
#include <string>
#include <vector>

#include "optimizer/expression.hpp"
#include "utils/assert.hpp"

namespace opossum {

UpdateNode::UpdateNode(const std::string& table_name,
                       const std::vector<std::shared_ptr<Expression>>& column_expressions)
    : AbstractASTNode(ASTNodeType::Update), _table_name(table_name), _column_expressions(column_expressions) {}

std::string UpdateNode::description() const {
  std::ostringstream desc;

  desc << "[Update] Table: '" << _table_name << "'";

  return desc.str();
}

const std::vector<std::shared_ptr<Expression>>& UpdateNode::column_expressions() const { return _column_expressions; }

const std::string& UpdateNode::table_name() const { return _table_name; }

void UpdateNode::map_column_ids(const ColumnIDMapping &column_id_mapping,
                                const std::optional<ASTChildSide> &caller_child_side) {
  for (const auto & column_expression : _column_expressions) {
    column_expression->map_column_ids(column_id_mapping);
  }
}

}  // namespace opossum
