#include "mock_node.hpp"

#include <memory>
#include <string>
#include <vector>

#include "expression/lqp_column_expression.hpp"
#include "utils/assert.hpp"

using namespace std::string_literals;  // NOLINT

namespace opossum {

MockNode::MockNode(const ColumnDefinitions& column_definitions, const std::optional<std::string>& name)
    : AbstractLQPNode(LQPNodeType::Mock), name(name), _column_definitions(column_definitions) {}

LQPColumnReference MockNode::get_column(const std::string& column_name) const {
  const auto& column_definitions = this->column_definitions();

  for (auto column_id = ColumnID{0}; column_id < column_definitions.size(); ++column_id) {
    if (column_definitions[column_id].second == column_name) return LQPColumnReference{shared_from_this(), column_id};
  }

  Fail("Couldn't find column named '"s + column_name + "' in MockNode");
}

const MockNode::ColumnDefinitions& MockNode::column_definitions() const { return _column_definitions; }

const std::vector<std::shared_ptr<AbstractExpression>>& MockNode::column_expressions() const {
  // Need to initialize the expressions lazily because they will have a weak_ptr to this node and we can't obtain that
  // in the constructor
  if (!_column_expressions) {
    _column_expressions.emplace(_column_definitions.size() - _pruned_column_ids.size());

    auto pruned_column_ids_iter = _pruned_column_ids.begin();

    auto output_column_id = ColumnID{0};
    for (auto stored_column_id = ColumnID{0}; stored_column_id < _column_definitions.size(); ++stored_column_id) {
      // Skip `stored_column_id` if it is in the sorted vector `_pruned_column_ids`
      if (pruned_column_ids_iter != _pruned_column_ids.end() && stored_column_id == *pruned_column_ids_iter) {
        ++pruned_column_ids_iter;
        continue;
      }

      (*_column_expressions)[output_column_id] =
          std::make_shared<LQPColumnExpression>(LQPColumnReference{shared_from_this(), stored_column_id});
      ++output_column_id;
    }
  }

  return *_column_expressions;
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

  // Rebuilding this lazily the next time `column_expressions()` is called
  _column_expressions.reset();
}

const std::vector<ColumnID>& MockNode::pruned_column_ids() const { return _pruned_column_ids; }

std::string MockNode::description() const {
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

size_t MockNode::_shallow_hash() const {
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
  const auto mock_node = MockNode::make(_column_definitions);
  mock_node->set_table_statistics(_table_statistics);
  mock_node->set_pruned_column_ids(_pruned_column_ids);
  mock_node->name = name;
  return mock_node;
}

bool MockNode::_on_shallow_equals(const AbstractLQPNode& rhs, const LQPNodeMapping& node_mapping) const {
  const auto& mock_node = static_cast<const MockNode&>(rhs);
  return _column_definitions == mock_node._column_definitions && _pruned_column_ids == mock_node._pruned_column_ids &&
         mock_node.name == name;
}

}  // namespace opossum
