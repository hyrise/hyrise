#include "mock_node.hpp"

#include <memory>
#include <string>
#include <vector>

#include "expression/lqp_cxlumn_expression.hpp"
#include "statistics/table_statistics.hpp"
#include "utils/assert.hpp"

using namespace std::string_literals;  // NOLINT

namespace opossum {

MockNode::MockNode(const CxlumnDefinitions& cxlumn_definitions, const std::optional<std::string>& name)
    : AbstractLQPNode(LQPNodeType::Mock), _name(name), _cxlumn_definitions(cxlumn_definitions) {}

LQPCxlumnReference MockNode::get_cxlumn(const std::string& name) const {
  const auto& cxlumn_definitions = this->cxlumn_definitions();

  for (auto cxlumn_id = CxlumnID{0}; cxlumn_id < cxlumn_definitions.size(); ++cxlumn_id) {
    if (cxlumn_definitions[cxlumn_id].second == name) return LQPCxlumnReference{shared_from_this(), cxlumn_id};
  }

  Fail("Couldn't find cxlumn named '"s + name + "' in MockNode");
}

const MockNode::CxlumnDefinitions& MockNode::cxlumn_definitions() const { return _cxlumn_definitions; }

const std::vector<std::shared_ptr<AbstractExpression>>& MockNode::cxlumn_expressions() const {
  if (!_cxlumn_expressions) {
    _cxlumn_expressions.emplace();

    const auto cxlumn_count = _cxlumn_definitions.size();

    for (auto cxlumn_id = CxlumnID{0}; cxlumn_id < cxlumn_count; ++cxlumn_id) {
      const auto cxlumn_reference = LQPCxlumnReference(shared_from_this(), cxlumn_id);
      _cxlumn_expressions->emplace_back(std::make_shared<LQPCxlumnExpression>(cxlumn_reference));
    }
  }

  return *_cxlumn_expressions;
}

std::string MockNode::description() const { return "[MockNode '"s + _name.value_or("Unnamed") + "']"; }

std::shared_ptr<TableStatistics> MockNode::derive_statistics_from(
    const std::shared_ptr<AbstractLQPNode>& left_input, const std::shared_ptr<AbstractLQPNode>& right_input) const {
  Assert(_table_statistics, "MockNode statistics need to be explicitely set");
  return _table_statistics;
}

void MockNode::set_statistics(const std::shared_ptr<TableStatistics>& statistics) { _table_statistics = statistics; }

std::shared_ptr<AbstractLQPNode> MockNode::_on_shallow_copy(LQPNodeMapping& node_mapping) const {
  const auto mock_node = MockNode::make(_cxlumn_definitions);
  mock_node->set_statistics(_table_statistics);
  return mock_node;
}

bool MockNode::_on_shallow_equals(const AbstractLQPNode& rhs, const LQPNodeMapping& node_mapping) const {
  const auto& mock_node = static_cast<const MockNode&>(rhs);
  return _cxlumn_definitions == mock_node._cxlumn_definitions && _table_statistics == mock_node._table_statistics;
}

}  // namespace opossum
