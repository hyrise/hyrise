#include "static_table_node.hpp"

#include <cstddef>
#include <functional>
#include <memory>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

#include <boost/container_hash/hash.hpp>

#include "expression/lqp_column_expression.hpp"
#include "logical_query_plan/abstract_lqp_node.hpp"
#include "logical_query_plan/data_dependencies/order_dependency.hpp"
#include "logical_query_plan/data_dependencies/unique_column_combination.hpp"
#include "lqp_utils.hpp"
#include "types.hpp"
#include "utils/assert.hpp"
#include "utils/print_utils.hpp"

namespace hyrise {

StaticTableNode::StaticTableNode(const std::shared_ptr<Table>& init_table)
    : AbstractLQPNode(LQPNodeType::StaticTable), table(init_table) {}

std::string StaticTableNode::description(const DescriptionMode /*mode*/) const {
  auto stream = std::ostringstream{};

  stream << "[StaticTable]:"
         << " (";
  for (auto column_id = ColumnID{0}; column_id < table->column_definitions().size(); ++column_id) {
    const auto& column_definition = table->column_definitions()[column_id];
    stream << column_definition;

    if (column_id + size_t{1} < table->column_definitions().size()) {
      stream << ", ";
    }
  }

  if (!table->soft_key_constraints().empty()) {
    stream << ", ";
    print_table_key_constraints(table, stream);
  }
  stream << ")";

  return stream.str();
}

std::vector<std::shared_ptr<AbstractExpression>> StaticTableNode::output_expressions() const {
  // Need to initialize the expressions lazily because they will have a weak_ptr to this node and we can't obtain
  // that in the constructor
  if (!_output_expressions) {
    const auto column_count = table->column_count();
    _output_expressions.emplace(column_count);

    for (auto column_id = ColumnID{0}; column_id < column_count; ++column_id) {
      (*_output_expressions)[column_id] = std::make_shared<LQPColumnExpression>(shared_from_this(), column_id);
    }
  }

  return *_output_expressions;
}

UniqueColumnCombinations StaticTableNode::unique_column_combinations() const {
  // Generate from table key constraints.
  auto unique_column_combinations = UniqueColumnCombinations{};
  const auto table_key_constraints = table->soft_key_constraints();

  for (const auto& table_key_constraint : table_key_constraints) {
    auto column_expressions = find_column_expressions(*this, table_key_constraint.columns());
    DebugAssert(column_expressions.size() == table_key_constraint.columns().size(),
                "Unexpected count of column expressions.");
    unique_column_combinations.emplace(std::move(column_expressions));
  }

  return unique_column_combinations;
}

OrderDependencies StaticTableNode::order_dependencies() const {
  return OrderDependencies{};
}

bool StaticTableNode::is_column_nullable(const ColumnID column_id) const {
  return table->column_is_nullable(column_id);
}

size_t StaticTableNode::_on_shallow_hash() const {
  auto hash = size_t{0};
  for (const auto& column_definition : table->column_definitions()) {
    boost::hash_combine(hash, column_definition.hash());
  }
  const auto& soft_key_constraints = table->soft_key_constraints();
  for (const auto& table_key_constraint : soft_key_constraints) {
    // To make the hash independent of the expressions' order, we have to use a commutative operator like XOR.
    hash = hash ^ table_key_constraint.hash();
  }

  return std::hash<size_t>{}(hash - soft_key_constraints.size());
}

std::shared_ptr<AbstractLQPNode> StaticTableNode::_on_shallow_copy(LQPNodeMapping& /*node_mapping*/) const {
  return StaticTableNode::make(table);
}

bool StaticTableNode::_on_shallow_equals(const AbstractLQPNode& rhs, const LQPNodeMapping& /*node_mapping*/) const {
  const auto& static_table_node = static_cast<const StaticTableNode&>(rhs);
  return table->column_definitions() == static_table_node.table->column_definitions() &&
         table->soft_key_constraints() == static_table_node.table->soft_key_constraints();
}

}  // namespace hyrise
