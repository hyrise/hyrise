#include "join_graph_visualizer.hpp"

#include <sstream>

#include "viz_record_layout.hpp"

namespace opossum {

void JoinGraphVisualizer::_build_graph(const std::shared_ptr<const JoinGraph>& graph) {
  for (auto vertex_idx = size_t{0}; vertex_idx < graph->vertices.size(); ++vertex_idx) {
    const auto& vertex = graph->vertices[vertex_idx];
    const auto predicates = graph->find_local_predicates(vertex_idx);

    VizRecordLayout layout;
    layout.add_label(vertex->description());

    if (!predicates.empty()) {
      auto& predicates_layout = layout.add_sublayout();

      for (const auto& predicate : predicates) {
        predicates_layout.add_label(predicate->as_column_name());
      }
    }

    VizVertexInfo vertex_info = _default_vertex;
    vertex_info.label = layout.to_label_string();
    vertex_info.shape = "record";

    _add_vertex(vertex, vertex_info);
  }

  for (const auto& edge : graph->edges) {
    const auto vertex_count = edge.vertex_set.count();

    // Single-vertex edges are local predicates. We already visualized those aboce
    if (vertex_count <= 1) continue;

    // Binary vertex edges: render a simple edge between the two vertices with the predicates as the edge label
    if (vertex_count == 2) {
      const auto first_vertex_idx = edge.vertex_set.find_first();
      const auto second_vertex_idx = edge.vertex_set.find_next(first_vertex_idx);

      const auto first_vertex = graph->vertices[first_vertex_idx];
      const auto second_vertex = graph->vertices[second_vertex_idx];

      std::stringstream edge_label_stream;
      for (const auto& predicate : edge.predicates) {
        edge_label_stream << predicate;
        edge_label_stream << "\n";
      }

      VizEdgeInfo edge_info;
      edge_info.color = _random_color();
      edge_info.font_color = edge_info.color;
      edge_info.dir = "none";
      edge_info.label = edge_label_stream.str();

      _add_edge(first_vertex, second_vertex, edge_info);
    } else {
      // More than two vertices, i.e. we have a hyperedge. Render a diamond vertex that contains all the Predicates and
      // connect all hyperedge vertices to that vertex.

      std::stringstream vertex_label_stream;
      for (size_t predicate_idx{0}; predicate_idx < edge.predicates.size(); ++predicate_idx) {
        const auto& predicate = edge.predicates[predicate_idx];
        vertex_label_stream << predicate->as_column_name();
        if (predicate_idx + 1 < edge.predicates.size()) {
          vertex_label_stream << "\n";
        }
      }

      VizVertexInfo vertex_info = _default_vertex;
      vertex_info.label = vertex_label_stream.str();
      vertex_info.color = _random_color();
      vertex_info.font_color = vertex_info.color;
      vertex_info.shape = "diamond";
      _add_vertex(edge, vertex_info);

      for (auto current_vertex_idx = edge.vertex_set.find_first(); current_vertex_idx != JoinGraphVertexSet::npos;
           current_vertex_idx = edge.vertex_set.find_next(current_vertex_idx)) {
        VizEdgeInfo edge_info;
        edge_info.dir = "none";
        edge_info.font_color = vertex_info.color;
        edge_info.color = vertex_info.color;
        _add_edge(graph->vertices[current_vertex_idx], edge, edge_info);
      }
    }
  }
}

}  // namespace opossum
