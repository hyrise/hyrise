#include "prepare_statement_node.hpp"

#include <sstream>

#include "storage/prepared_plan.hpp"

namespace opossum {

PrepareStatementNode::PrepareStatementNode(const std::string& name,
                                           const std::shared_ptr<PreparedPlan>& prepared_plan)
    : BaseNonQueryNode(LQPNodeType::PrepareStatement), name(name), prepared_plan(prepared_plan) {}

std::string PrepareStatementNode::description() const {
  std::stringstream stream;
  stream << "PrepareStatement '" << name << "' (" << reinterpret_cast<const void*>(prepared_plan->lqp.get())
         << ") ";
  stream << "{\n";
  prepared_plan->print(stream);
  stream << "}\n";

  return stream.str();
}

std::shared_ptr<AbstractLQPNode> PrepareStatementNode::_on_shallow_copy(LQPNodeMapping& node_mapping) const {
  return PrepareStatementNode::make(name, prepared_plan);
}

bool PrepareStatementNode::_on_shallow_equals(const AbstractLQPNode& rhs, const LQPNodeMapping& node_mapping) const {
  const auto& prepare_statement_node = static_cast<const PrepareStatementNode&>(rhs);
  return name == prepare_statement_node.name && *prepared_plan == *prepare_statement_node.prepared_plan;
}

}  // namespace opossum
