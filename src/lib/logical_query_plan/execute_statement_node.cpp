#include "execute_statement_node.hpp"

namespace opossum {

ExecuteStatementNode(const std::string& name, const std::vector<AllTypeVariant>& parameters);

std::string description() const;

std::shared_ptr<AbstractLQPNode> _on_shallow_copy(LQPNodeMapping& node_mapping) const;
bool _on_shallow_equals(const AbstractLQPNode& rhs, const LQPNodeMapping& node_mapping) const;

}  // namespace opossum
