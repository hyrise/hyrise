#include "mock_node.hpp"

#include <memory>
#include <string>
#include <vector>

#include "expression/lqp_column_expression.hpp"
#include "lqp_utils.hpp"
#include "utils/assert.hpp"

using namespace std::string_literals;  // NOLINT

namespace opossum {

MockNode::MockNode(const ColumnDefinitions& column_definitions, const std::optional<std::string>& init_name)
    : AbstractLQPNode(LQPNodeType::Mock), name(init_name), _column_definitions(column_definitions) {}

std::shared_ptr<LQPColumnExpression> MockNode::get_column(const std::string& column_name) const {
  const auto& column_definitions = this->column_definitions();

  for (auto column_id = ColumnID{0}; column_id < column_definitions.size(); ++column_id) {
    if (column_definitions[column_id].second == column_name) {
      return std::make_shared<LQPColumnExpression>(shared_from_this(), column_id);
    }
  }

  Fail("Couldn't find column named '"s + column_name + "' in MockNode");
}

const MockNode::ColumnDefinitions& MockNode::column_definitions() const { return _column_definitions; }

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
  Assert(column_id < _column_definitions.size(), "ColumnID out of range");
  return false;
}

void MockNode::set_pruned_column_ids(const std::vector<ColumnID>& pruned_column_ids) {
  DebugAssert(std::is_sorted(pruned_column_ids.begin(), pruned_column_ids.end()),
              "Expected sorted vector of ColumnIDs");
  DebugAssert(std::adjacent_find(pruned_column_ids.begin(), pruned_column_ids.end()) == pruned_column_ids.end(),
              "Expected vector of unique ColumnIDs");

  _pruned_column_ids = pruned_column_ids;

  // Rebuilding this lazily the next time `output_expressions()` is called
  _output_expressions.reset();
}

std::shared_ptr<LQPUniqueConstraints> MockNode::unique_constraints() const {
  auto unique_constraints = std::make_shared<LQPUniqueConstraints>();

  for (const auto& table_key_constraint : _table_key_constraints) {
    // Discard key constraints that involve pruned column id(s).
    const auto& key_constraint_column_ids = table_key_constraint.columns();
    if (std::any_of(_pruned_column_ids.cbegin(), _pruned_column_ids.cend(),
                    [&key_constraint_column_ids](const auto& pruned_column_id) {
                      return key_constraint_column_ids.contains(pruned_column_id);
                    })) {
      continue;
    }

    // Search for output expressions that represent the TableKeyConstraint's ColumnIDs
    const auto& column_expressions = find_column_expressions(*this, key_constraint_column_ids);
    DebugAssert(column_expressions.size() == table_key_constraint.columns().size(),
                "Unexpected count of column expressions.");

    // Create LQPUniqueConstraint
    unique_constraints->emplace_back(column_expressions);
  }

  return unique_constraints;
}

const std::vector<ColumnID>& MockNode::pruned_column_ids() const { return _pruned_column_ids; }

std::string MockNode::description(const DescriptionMode mode) const {
  std::ostringstream stream;
  stream << "[MockNode '"s << name.value_or("Unnamed") << "'] Columns:";

  auto column_id = ColumnID{0};
  for (const auto& column : _column_definitions) {
    if (std::find(_pruned_column_ids.begin(), _pruned_column_ids.end(), column_id) != _pruned_column_ids.end()) {
      ++column_id;
      continue;
    }
    stream << " " << column.second;
    ++column_id;
  }

  stream << " | pruned: " << _pruned_column_ids.size() << "/" << _column_definitions.size() << " columns";

  return stream.str();
}

const std::shared_ptr<TableStatistics>& MockNode::table_statistics() const { return _table_statistics; }

void MockNode::set_table_statistics(const std::shared_ptr<TableStatistics>& table_statistics) {
  _table_statistics = table_statistics;
}

void MockNode::set_key_constraints(const TableKeyConstraints& key_constraints) {
  _table_key_constraints = key_constraints;
}

const TableKeyConstraints& MockNode::key_constraints() const { return _table_key_constraints; }

void MockNode::set_non_trivial_functional_dependencies(const std::vector<FunctionalDependency>& fds) {
  _functional_dependencies = fds;
}

std::vector<FunctionalDependency> MockNode::non_trivial_functional_dependencies() const {
  return _functional_dependencies;
}

size_t MockNode::_on_shallow_hash() const {
  auto hash = boost::hash_value(_table_statistics);
  for (const auto& pruned_column_id : _pruned_column_ids) {
    boost::hash_combine(hash, static_cast<size_t>(pruned_column_id));
  }
  for (const auto& [type, column_name] : _column_definitions) {
    boost::hash_combine(hash, type);
    boost::hash_combine(hash, column_name);
  }
  return hash;
}

std::shared_ptr<AbstractLQPNode> MockNode::_on_shallow_copy(LQPNodeMapping& node_mapping) const {
  const auto mock_node = MockNode::make(_column_definitions, name);
  mock_node->set_table_statistics(_table_statistics);
  mock_node->set_key_constraints(_table_key_constraints);
  mock_node->set_non_trivial_functional_dependencies(_functional_dependencies);
  mock_node->set_pruned_column_ids(_pruned_column_ids);
  return mock_node;
}

bool MockNode::_on_shallow_equals(const AbstractLQPNode& rhs, const LQPNodeMapping& node_mapping) const {
  const auto& mock_node = static_cast<const MockNode&>(rhs);
  return _column_definitions == mock_node._column_definitions && _pruned_column_ids == mock_node._pruned_column_ids &&
         mock_node.name == name && mock_node.key_constraints() == _table_key_constraints &&
         mock_node.functional_dependencies() == _functional_dependencies;
}

}  // namespace opossum
