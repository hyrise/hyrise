#include "union_node.hpp"

#include <string>

#include "constant_mappings.hpp"

namespace opossum {

UnionNode::UnionNode(UnionMode union_mode) : AbstractASTNode(ASTNodeType::Union), _union_mode(union_mode) {}

UnionMode UnionNode::union_mode() const { return _union_mode; }

std::string UnionNode::description() const { return "UnionNode (" + union_mode_to_string.at(_union_mode) + ")"; }
}  // namespace opossum
