#include "join_graph_edge.hpp"

#include "expression/abstract_expression.hpp"

namespace opossum {

JoinGraphEdge::JoinGraphEdge(const JoinGraphVertexSet& vertex_set,
                             const std::vector<std::shared_ptr<AbstractExpression>>& predicates)
    : vertex_set(vertex_set), predicates(predicates) {}

std::ostream& operator<<(std::ostream& stream, const JoinGraphEdge& join_graph_edge) {
  stream << "Vertices: " << join_graph_edge.vertex_set << "; " << join_graph_edge.predicates.size() << " predicates"
         << std::endl;
  for (const auto& predicate : join_graph_edge.predicates) {
    stream << predicate->as_column_name() << std::endl;
  }
  return stream;
}

}  // namespace opossum
