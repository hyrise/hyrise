#include "limit_node.hpp"

#include <memory>
#include <string>

namespace opossum {

LimitNode::LimitNode(const size_t num_rows) : AbstractASTNode(ASTNodeType::Limit), _num_rows(num_rows) {}

std::string LimitNode::description() const { return "[Limit] " + std::to_string(_num_rows) + " rows"; }

size_t LimitNode::num_rows() const { return _num_rows; }

std::shared_ptr<AbstractASTNode> LimitNode::clone_subtree() const {
  return _clone_without_subclass_members<decltype(*this)>();
}

}  // namespace opossum
