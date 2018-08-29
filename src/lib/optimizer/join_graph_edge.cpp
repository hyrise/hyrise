#include "join_graph_edge.hpp"

#include "expression/abstract_expression.hpp"

namespace opossum {

JoinGraphEdge::JoinGraphEdge(const JoinGraphVertexSet& vertex_set,
                             const std::vector<std::shared_ptr<AbstractExpression>>& predicates)
    : vertex_set(vertex_set), predicates(predicates) {}

void JoinGraphEdge::print(std::ostream& stream) const {
  stream << "Vertices: " << vertex_set << "; " << predicates.size() << " predicates" << std::endl;
  for (const auto& predicate : predicates) {
    stream << predicate->as_column_name();
    stream << std::endl;
  }
}
}  // namespace opossum
