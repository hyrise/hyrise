#include "mock_node.hpp"

#include <algorithm>
#include <cstddef>
#include <memory>
#include <optional>
#include <ostream>
#include <sstream>
#include <string>
#include <vector>

#include <boost/container_hash/hash.hpp>

#include "expression/expression_utils.hpp"
#include "expression/lqp_column_expression.hpp"
#include "logical_query_plan/abstract_lqp_node.hpp"
#include "logical_query_plan/data_dependencies/functional_dependency.hpp"
#include "logical_query_plan/data_dependencies/unique_column_combination.hpp"
#include "lqp_utils.hpp"
#include "storage/constraints/table_key_constraint.hpp"
#include "types.hpp"
#include "utils/assert.hpp"

namespace {

using namespace hyrise;  // NOLINT(build/namespaces)

bool contains_column(const std::vector<ColumnID>& column_ids, const ColumnID search_column_id) {
  return std::find(column_ids.cbegin(), column_ids.cend(), search_column_id) != column_ids.cend();
}

template <typename ColumnIDs>
bool contains_any_column(const std::vector<ColumnID>& column_ids, const ColumnIDs& search_column_ids) {
  return std::any_of(search_column_ids.cbegin(), search_column_ids.cend(), [&](const auto column_id) {
    return contains_column(column_ids, column_id);
  });
}

}  // namespace

namespace hyrise {

MockNode::MockNode(const ColumnDefinitions& column_definitions, const std::optional<std::string>& init_name)
    : AbstractLQPNode(LQPNodeType::Mock), name(init_name), _column_definitions(column_definitions) {}

std::shared_ptr<LQPColumnExpression> MockNode::get_column(const std::string& column_name) const {
  const auto& column_definitions = this->column_definitions();

  for (auto column_id = ColumnID{0}; column_id < column_definitions.size(); ++column_id) {
    if (column_definitions[column_id].second == column_name) {
      return std::make_shared<LQPColumnExpression>(shared_from_this(), column_id);
    }
  }

  Fail("Could not find column named '" + column_name + "' in MockNode.");
}

const MockNode::ColumnDefinitions& MockNode::column_definitions() const {
  return _column_definitions;
}

std::vector<std::shared_ptr<AbstractExpression>> MockNode::output_expressions() const {
  // Need to initialize the expressions lazily because they will have a weak_ptr to this node and we can't obtain that
  // in the constructor
  if (!_output_expressions) {
    _output_expressions.emplace(_column_definitions.size() - _pruned_column_ids.size());

    auto pruned_column_ids_iter = _pruned_column_ids.begin();

    auto output_column_id = ColumnID{0};
    for (auto stored_column_id = ColumnID{0}; stored_column_id < _column_definitions.size(); ++stored_column_id) {
      // Skip `stored_column_id` if it is in the sorted vector `_pruned_column_ids`
      if (pruned_column_ids_iter != _pruned_column_ids.end() && stored_column_id == *pruned_column_ids_iter) {
        ++pruned_column_ids_iter;
        continue;
      }

      (*_output_expressions)[output_column_id] =
          std::make_shared<LQPColumnExpression>(shared_from_this(), stored_column_id);
      ++output_column_id;
    }
  }

  return *_output_expressions;
}

bool MockNode::is_column_nullable(const ColumnID column_id) const {
  Assert(column_id < _column_definitions.size(), "ColumnID out of range.");
  return false;
}

void MockNode::set_pruned_column_ids(const std::vector<ColumnID>& pruned_column_ids) {
  DebugAssert(std::is_sorted(pruned_column_ids.begin(), pruned_column_ids.end()),
              "Expected sorted vector of ColumnIDs.");
  DebugAssert(std::adjacent_find(pruned_column_ids.begin(), pruned_column_ids.end()) == pruned_column_ids.end(),
              "Expected vector of unique ColumnIDs.");

  _pruned_column_ids = pruned_column_ids;

  // Rebuilding this lazily the next time `output_expressions()` is called
  _output_expressions.reset();
}

const std::vector<ColumnID>& MockNode::pruned_column_ids() const {
  return _pruned_column_ids;
}

std::string MockNode::description(const DescriptionMode /*mode*/) const {
  auto stream = std::ostringstream{};
  stream << "[MockNode '" << name.value_or("Unnamed") << "'] Columns:";

  auto column_id = ColumnID{0};
  for (const auto& column : _column_definitions) {
    if (contains_column(_pruned_column_ids, column_id)) {
      ++column_id;
      continue;
    }
    stream << " " << column.second;
    ++column_id;
  }

  stream << " | pruned: " << _pruned_column_ids.size() << "/" << _column_definitions.size() << " columns";

  return stream.str();
}

UniqueColumnCombinations MockNode::unique_column_combinations() const {
  auto unique_column_combinations = UniqueColumnCombinations{};

  for (const auto& table_key_constraint : _table_key_constraints) {
    // Discard key constraints that involve pruned column id(s).
    const auto& key_constraint_column_ids = table_key_constraint.columns();
    if (contains_any_column(_pruned_column_ids, key_constraint_column_ids)) {
      continue;
    }

    // Search for output expressions that represent the TableKeyConstraint's ColumnIDs.
    const auto& column_expressions = find_column_expressions(*this, key_constraint_column_ids);
    DebugAssert(column_expressions.size() == table_key_constraint.columns().size(),
                "Unexpected count of column expressions.");

    // Create UniqueColumnCombination.
    unique_column_combinations.emplace(column_expressions);
  }

  return unique_column_combinations;
}

void MockNode::set_order_constraints(const TableOrderConstraints& order_constraints) {
  _order_constraints = order_constraints;
}

OrderDependencies MockNode::order_dependencies() const {
  auto order_dependencies = OrderDependencies{};
  for (const auto& order_constraint : _order_constraints) {
    const auto& ordering_columns = order_constraint.ordering_columns();
    const auto& ordered_columns = order_constraint.ordered_columns();
    if (contains_any_column(_pruned_column_ids, ordering_columns) ||
        contains_any_column(_pruned_column_ids, ordered_columns)) {
      continue;
    }

    order_dependencies.emplace(find_column_expressions(*this, ordering_columns),
                               find_column_expressions(*this, ordered_columns));
  }
  return order_dependencies;
}

const std::shared_ptr<TableStatistics>& MockNode::table_statistics() const {
  return _table_statistics;
}

void MockNode::set_table_statistics(const std::shared_ptr<TableStatistics>& table_statistics) {
  _table_statistics = table_statistics;
}

void MockNode::set_key_constraints(const TableKeyConstraints& key_constraints) {
  _table_key_constraints = key_constraints;
}

const TableKeyConstraints& MockNode::key_constraints() const {
  return _table_key_constraints;
}

void MockNode::set_non_trivial_functional_dependencies(const FunctionalDependencies& fds) {
  _functional_dependencies = fds;
}

FunctionalDependencies MockNode::non_trivial_functional_dependencies() const {
  return _functional_dependencies;
}

size_t MockNode::_on_shallow_hash() const {
  auto hash = size_t{0};
  boost::hash_combine(hash, _table_statistics);
  for (const auto& pruned_column_id : _pruned_column_ids) {
    boost::hash_combine(hash, pruned_column_id);
  }
  for (const auto& [type, column_name] : _column_definitions) {
    boost::hash_combine(hash, type);
    boost::hash_combine(hash, column_name);
  }
  return hash;
}

std::shared_ptr<AbstractLQPNode> MockNode::_on_shallow_copy(LQPNodeMapping& /*node_mapping*/) const {
  const auto mock_node = MockNode::make(_column_definitions, name);
  mock_node->set_table_statistics(_table_statistics);
  mock_node->set_key_constraints(_table_key_constraints);
  mock_node->set_order_constraints(_order_constraints);
  mock_node->set_non_trivial_functional_dependencies(_functional_dependencies);
  mock_node->set_pruned_column_ids(_pruned_column_ids);
  return mock_node;
}

bool MockNode::_on_shallow_equals(const AbstractLQPNode& rhs, const LQPNodeMapping& /*node_mapping*/) const {
  const auto& mock_node = static_cast<const MockNode&>(rhs);

  return _column_definitions == mock_node._column_definitions && _pruned_column_ids == mock_node._pruned_column_ids &&
         mock_node.name == name && mock_node.key_constraints() == _table_key_constraints &&
         mock_node._functional_dependencies == _functional_dependencies;
}

}  // namespace hyrise
