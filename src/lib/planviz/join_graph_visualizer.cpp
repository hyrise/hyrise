#include "join_graph_visualizer.hpp"

#include <fstream>

#include "boost/algorithm/string.hpp"

#include "optimizer/abstract_syntax_tree/stored_table_node.hpp"
#include "optimizer/join_graph.hpp"
#include "constant_mappings.hpp"
#include "ast_visualizer.hpp"
#include "graphviz_tools.hpp"

namespace opossum {

JoinGraphVisualizer::JoinGraphVisualizer(const DotConfig &config): _config(config) {

}

void JoinGraphVisualizer::visualize(const std::shared_ptr<JoinGraph> &join_graph,
                                    const std::string &output_prefix) {
  std::ofstream file;
  file.open(output_prefix + ".dot");
  file << "digraph {" << std::endl;
  file << "rankdir=BT" << std::endl;
  file << "bgcolor=" << dot_color_to_string.at(_config.background_color) << std::endl;
  file << "node [color=white,fontcolor=white,shape=box]" << std::endl;
  file << "edge [color=white,fontcolor=white]" << std::endl;

  // Vertices
  file << "subgraph {" << std::endl;
  file << "node [fontsize=18,shape=record]" << std::endl;
  file << "edge [dir=none]" << std::endl;
  for (size_t join_vertex_idx = 0; join_vertex_idx < join_graph->vertices().size(); ++join_vertex_idx) {
    const auto& join_vertex = join_graph->vertices()[join_vertex_idx];

    /**
     * Generate VertexPredicate descriptions
     */
    std::vector<std::string> predicate_descriptions;
    for (const auto& predicate : join_vertex.predicates) {
      const auto description =
        join_vertex.node->get_qualified_column_name(predicate.column_id) + " " +
        scan_type_to_string.left.at(predicate.scan_type) + " " + boost::lexical_cast<std::string>(predicate.value);
      predicate_descriptions.emplace_back(description);
    }

    /**
     * Generate Vertex label - use description of root of subtree
     */
    std::string label = join_vertex.node->description();

    file << reinterpret_cast<uintptr_t>(&join_vertex) << "[label=\"{" << label;
    if (!predicate_descriptions.empty()) {
      file << " | { Predicates | {";
      for (size_t description_idx = 0; description_idx < predicate_descriptions.size(); ++description_idx) {
        file << predicate_descriptions[description_idx];
        if (description_idx + 1 < predicate_descriptions.size()) {
          file << " | ";
        }
      }
      file << "} }";
    }
    file << "}\"]";
    file << std::endl;
  }

  /**
   * CrossJoin edges
   */
  file << "edge [style=dotted,penwidth=2]" << std::endl;
  for (size_t join_edge_idx = 0; join_edge_idx < join_graph->edges().size(); ++join_edge_idx) {
    const auto& join_edge = join_graph->edges()[join_edge_idx];
    if (join_edge.join_mode != JoinMode::Cross) {
      continue;
    }

    file << reinterpret_cast<uintptr_t>(&join_graph->vertices()[join_edge.vertex_ids.first]);
    file << " -> ";
    file << reinterpret_cast<uintptr_t>(&join_graph->vertices()[join_edge.vertex_ids.second]);
    file << std::endl;
  }

  /**
   * PredicateJoin edges
   */
  file << "edge [style=solid,penwidth=3]" << std::endl;
  for (size_t join_edge_idx = 0; join_edge_idx < join_graph->edges().size(); ++join_edge_idx) {
    const auto& join_edge = join_graph->edges()[join_edge_idx];
    if (join_edge.join_mode == JoinMode::Cross) {
      continue;
    }

    file << reinterpret_cast<uintptr_t>(&join_graph->vertices()[join_edge.vertex_ids.first]);
    file << " -> ";
    file << reinterpret_cast<uintptr_t>(&join_graph->vertices()[join_edge.vertex_ids.second]);

    const auto left_column_name = join_graph->vertices()[join_edge.vertex_ids.first].node->get_qualified_column_name(join_edge.column_ids->first);
    const auto right_column_name = join_graph->vertices()[join_edge.vertex_ids.second].node->get_qualified_column_name(join_edge.column_ids->second);
    const auto scan_type_name = scan_type_to_string.left.at(*join_edge.scan_type);

    const auto color = graphviz_random_color();

    file << "[";
    file << "label=\"" << left_column_name << "\\n";
    file << scan_type_name << "\\n";
    file << right_column_name << "\"";
    file << ",";
    file << "color=\"" << color << "\"";
    file << "fontcolor=\"" << color << "\"";
    file << "]";
    file << std::endl;
  }

  file << "}" << std::endl;

  file << "}";
  file.close();

  graphviz_call_cmd(GraphvizLayout::Circo, _config.render_format, output_prefix);
}

}