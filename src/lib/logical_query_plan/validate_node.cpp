#include "validate_node.hpp" // NEEDEDINCLUDE


#include "statistics/table_statistics.hpp" // NEEDEDINCLUDE

namespace opossum {

ValidateNode::ValidateNode() : AbstractLQPNode(LQPNodeType::Validate) {}

std::string ValidateNode::description() const { return "[Validate]"; }

std::shared_ptr<AbstractLQPNode> ValidateNode::_on_shallow_copy(LQPNodeMapping& node_mapping) const {
  return ValidateNode::make();
}

std::shared_ptr<TableStatistics> ValidateNode::derive_statistics_from(
    const std::shared_ptr<AbstractLQPNode>& left_input, const std::shared_ptr<AbstractLQPNode>& right_input) const {
  DebugAssert(left_input && !right_input, "ValidateNode needs left_input and no right_input");

  auto input_statistics = left_input->get_statistics();

  return std::make_shared<TableStatistics>(input_statistics->table_type(), input_statistics->approx_valid_row_count(),
                                           input_statistics->column_statistics());
}

bool ValidateNode::_on_shallow_equals(const AbstractLQPNode& rhs, const LQPNodeMapping& node_mapping) const {
  return true;
}

}  // namespace opossum
