#include "limit_node.hpp"

#include <string>

namespace opossum {

LimitNode::LimitNode(const size_t num_rows) : AbstractASTNode(ASTNodeType::Limit), _num_rows(num_rows) {}

std::string LimitNode::description() const { return "Limit: " + std::to_string(_num_rows); }

size_t LimitNode::num_rows() const { return _num_rows; }

}  // namespace opossum
