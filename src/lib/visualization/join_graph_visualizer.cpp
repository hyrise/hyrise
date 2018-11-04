#include "join_graph_visualizer.hpp"

#include <sstream>

#include "logical_query_plan/lqp_utils.hpp"
#include "logical_query_plan/stored_table_node.hpp"
#include "viz_record_layout.hpp"

namespace opossum {

void JoinGraphVisualizer::_build_graph(const std::vector<JoinGraph>& graphs) {
  for (const auto& graph : graphs) {
    for (auto vertex_idx = size_t{0}; vertex_idx < graph.vertices.size(); ++vertex_idx) {
      const auto& vertex = graph.vertices[vertex_idx];
      const auto predicates = graph.find_local_predicates(vertex_idx);

      VizRecordLayout layout;
      layout.add_label(_create_vertex_description(vertex));

      if (!predicates.empty()) {
        auto& predicates_layout = layout.add_sublayout();

        for (const auto& predicate : predicates) {
          predicates_layout.add_label(predicate->as_column_name());
        }
      }

      VizVertexInfo vertex_info = _default_vertex;
      vertex_info.label = layout.to_label_string();
      vertex_info.shape = "record";

      // Don't wrap the label, we're using the record layout, which has internal formatting. Randomly inserted newlines
      // wouldn't help here
      _add_vertex(vertex, vertex_info, WrapLabel::Off);
    }

    for (const auto& edge : graph.edges) {
      const auto vertex_count = edge.vertex_set.count();

      // Single-vertex edges are local predicates. We already visualized those above
      if (vertex_count <= 1) continue;

      // Binary vertex edges: render a simple edge between the two vertices with the predicates as the edge label
      if (vertex_count == 2) {
        const auto first_vertex_idx = edge.vertex_set.find_first();
        const auto second_vertex_idx = edge.vertex_set.find_next(first_vertex_idx);

        const auto first_vertex = graph.vertices[first_vertex_idx];
        const auto second_vertex = graph.vertices[second_vertex_idx];

        std::stringstream edge_label_stream;
        for (const auto& predicate : edge.predicates) {
          edge_label_stream << predicate->as_column_name();
          edge_label_stream << "\n";
        }

        VizEdgeInfo edge_info;
        edge_info.color = _random_color();
        edge_info.font_color = edge_info.color;
        edge_info.dir = "none";
        edge_info.label = edge_label_stream.str();

        _add_edge(first_vertex, second_vertex, edge_info);
      } else {
        // More than two vertices, i.e. we have a hyperedge (think `SELECT * FROM x, y, z WHERE x.a + y.b + z.c = x.d`.)
        // Render a diamond vertex that contains all the Predicates and connect all hyperedge vertices to that vertex.

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
          _add_edge(graph.vertices[current_vertex_idx], edge, edge_info);
        }
      }
    }
  }
}

std::string JoinGraphVisualizer::_create_vertex_description(const std::shared_ptr<AbstractLQPNode>& vertex) {
  auto stored_table_nodes = std::vector<std::shared_ptr<StoredTableNode>>{};

  std::ostringstream stream;

  visit_lqp(vertex, [&](const auto& sub_node) {
    if (const auto stored_table_node = std::dynamic_pointer_cast<StoredTableNode>(sub_node)) {
      stored_table_nodes.emplace_back(stored_table_node);
    }

    return LQPVisitation::VisitInputs;
  });

  if (stored_table_nodes.empty()) {
    stream << "No Tables";
  } else if (stored_table_nodes.size() == 1) {
    stream << "Table: ";
  } else {
    stream << "Tables: ";
  }

  for (auto node_idx = size_t{0}; node_idx < stored_table_nodes.size(); ++node_idx) {
    stream << stored_table_nodes[node_idx]->table_name;
    if (node_idx + 1u < stored_table_nodes.size()) stream << ", ";
  }

  return stream.str();
}

}  // namespace opossum
