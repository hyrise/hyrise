#include "static_table_node.hpp"

#include <sstream>

#include "constant_mappings.hpp"
#include "expression/lqp_column_expression.hpp"
#include "lqp_utils.hpp"

namespace opossum {

StaticTableNode::StaticTableNode(const std::shared_ptr<Table>& init_table)
    : AbstractLQPNode(LQPNodeType::StaticTable), table(init_table) {}

std::string StaticTableNode::description(const DescriptionMode mode) const {
  std::ostringstream stream;

  stream << "[StaticTable]:"
         << " (";
  for (auto column_id = ColumnID{0}; column_id < table->column_definitions().size(); ++column_id) {
    const auto& column_definition = table->column_definitions()[column_id];
    stream << column_definition;

    if (column_id + 1u < table->column_definitions().size()) {
      stream << ", ";
    }
  }
  stream << ")";

  return stream.str();
}

std::vector<std::shared_ptr<AbstractExpression>> StaticTableNode::output_expressions() const {
  // Need to initialize the expressions lazily because they will have a weak_ptr to this node and we can't obtain
  // that in the constructor
  if (!_output_expressions) {
    _output_expressions.emplace(table->column_count());

    for (auto column_id = ColumnID{0}; column_id < table->column_count(); ++column_id) {
      (*_output_expressions)[column_id] = std::make_shared<LQPColumnExpression>(shared_from_this(), column_id);
    }
  }

  return *_output_expressions;
}

std::shared_ptr<LQPUniqueConstraints> StaticTableNode::unique_constraints() const {
  // Generate from table key constraints
  auto unique_constraints = std::make_shared<LQPUniqueConstraints>();
  const auto table_key_constraints = table->soft_key_constraints();

  for (const auto& table_key_constraint : table_key_constraints) {
    const auto& column_expressions = find_column_expressions(*this, table_key_constraint.columns());
    DebugAssert(column_expressions.size() == table_key_constraint.columns().size(),
                "Unexpected count of column expressions.");
    unique_constraints->emplace_back(column_expressions);
  }

  return unique_constraints;
}

bool StaticTableNode::is_column_nullable(const ColumnID column_id) const {
  return table->column_is_nullable(column_id);
}

size_t StaticTableNode::_on_shallow_hash() const {
  size_t hash{0};
  for (const auto& column_definition : table->column_definitions()) {
    boost::hash_combine(hash, column_definition.hash());
  }
  return hash;
}

std::shared_ptr<AbstractLQPNode> StaticTableNode::_on_shallow_copy(LQPNodeMapping& node_mapping) const {
  return StaticTableNode::make(table);
}

bool StaticTableNode::_on_shallow_equals(const AbstractLQPNode& rhs, const LQPNodeMapping& node_mapping) const {
  const auto& static_table_node = static_cast<const StaticTableNode&>(rhs);
  return table->column_definitions() == static_table_node.table->column_definitions();
}

}  // namespace opossum
