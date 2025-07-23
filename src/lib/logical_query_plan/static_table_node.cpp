#include "static_table_node.hpp"

#include <cstddef>
#include <memory>
#include <sstream>
#include <string>
#include <unordered_set>
#include <utility>
#include <vector>

#include <boost/container_hash/hash.hpp>

#include "expression/lqp_column_expression.hpp"
#include "logical_query_plan/abstract_lqp_node.hpp"
#include "logical_query_plan/data_dependencies/order_dependency.hpp"
#include "logical_query_plan/data_dependencies/unique_column_combination.hpp"
#include "lqp_utils.hpp"
#include "storage/constraints/constraint_utils.hpp"
#include "storage/constraints/table_key_constraint.hpp"
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

void StaticTableNode::_set_output_expressions() const {
  // Need to initialize the expressions lazily because they will have a weak_ptr to this node and we can't obtain
  // that in the constructor

  const auto column_count = table->column_count();
  _output_expressions.emplace(column_count);

  for (auto column_id = ColumnID{0}; column_id < column_count; ++column_id) {
    (*_output_expressions)[column_id] = std::make_shared<LQPColumnExpression>(shared_from_this(), column_id);
  }
}

UniqueColumnCombinations StaticTableNode::unique_column_combinations() const {
  // Generate from table key constraints.
  auto unique_column_combinations = UniqueColumnCombinations{};
  const auto& table_key_constraints = table->soft_key_constraints();

  for (const auto& table_key_constraint : table_key_constraints) {
    auto column_expressions = get_expressions_for_column_ids(*this, table_key_constraint.columns());
    DebugAssert(column_expressions.size() == table_key_constraint.columns().size(),
                "Unexpected count of column expressions.");
    if (key_constraint_is_confidently_valid(table, table_key_constraint)) {
      // We may only use the key constraints as UCCs for optimization purposes if they are certainly still valid,
      // otherwise these optimizations could produce invalid query results.
      unique_column_combinations.emplace(std::move(column_expressions));
    }
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
  auto hash = boost::hash_value(table->column_definitions().size());
  for (const auto& column_definition : table->column_definitions()) {
    boost::hash_combine(hash, column_definition.hash());
  }
  // We do not hash all key constraints because the cost of hashing outweights the benefit of less collisions. In the
  // case of collisions `_on_shallow_equals` will be called anyway.
  return hash;
}

std::shared_ptr<AbstractLQPNode> StaticTableNode::_on_shallow_copy(LQPNodeMapping& /*node_mapping*/) const {
  return StaticTableNode::make(table);
}

bool StaticTableNode::_on_shallow_equals(const AbstractLQPNode& rhs, const LQPNodeMapping& /*node_mapping*/) const {
  const auto& static_table_node = static_cast<const StaticTableNode&>(rhs);

  if (table->column_definitions() != static_table_node.table->column_definitions()) {
    return false;
  }

  // If the column definitions match, we also compare the key constraints. Note that this comparison does only compare
  // the type and columns of two key constraints, not the validity or stored CommitIDs. This is sufficient for the
  // StaticTableNode because it should not contain invalid key constraints in the first place.
  return table->soft_key_constraints() == static_table_node.table->soft_key_constraints();
}

}  // namespace hyrise
