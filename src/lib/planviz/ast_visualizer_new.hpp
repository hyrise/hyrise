#pragma once

#include <boost/algorithm/string.hpp>
#include <iomanip>
#include <memory>
#include <string>
#include <vector>

#include "optimizer/abstract_syntax_tree/abstract_ast_node.hpp"
#include "optimizer/table_statistics.hpp"
#include "planviz/abstract_visualizer.hpp"
#include "planviz/visualizer.hpp"

namespace opossum {

class ASTVisualizerNew : public AbstractVisualizer {
 public:

  ASTVisualizerNew() {
    VizVertexInfo vertex_info;
    vertex_info.shape = "parallelogram";

    ASTVisualizerNew(VizGraphInfo{}, vertex_info, VizEdgeInfo{});
  };

  ASTVisualizerNew(const VizGraphInfo& graph_info, const VizVertexInfo& vertex_info, const VizEdgeInfo& edge_info)
    : AbstractVisualizer(graph_info, vertex_info, edge_info) {};

  void build_graph(const std::vector<std::shared_ptr<AbstractASTNode>>& ast_roots) {
    for (const auto& root : ast_roots) {
      _build_subtree(root);
    }
  }

 protected:
  void _build_subtree(const std::shared_ptr<AbstractASTNode>& node) {
    _add_vertex(node, node->description());

    if (node->left_child()) {
      auto left_child = node->left_child();
      _add_vertex(left_child, left_child->description());
      _build_dataflow(left_child, node);
      _build_subtree(left_child);
    }

    if (node->right_child()) {
      auto right_child = node->right_child();
      _add_vertex(right_child, right_child->description());
      _build_dataflow(right_child, node);
      _build_subtree(right_child);
    }
  }


  void _build_dataflow(const std::shared_ptr<AbstractASTNode>& from, const std::shared_ptr<AbstractASTNode>& to) {
    float row_count, row_percentage = 100.0f;
    uint8_t pen_width;

    try {
      row_count = from->get_statistics()->row_count();
      pen_width = std::fmax(1, std::ceil(std::log10(row_count) / 2));
    } catch (...) {
      // statistics don't exist for this edge
      row_count = NAN;
      pen_width = 1;
    }

    if (from->left_child()) {
      try {
        float input_count = from->left_child()->get_statistics()->row_count();
        if (from->right_child()) {
          input_count *= from->right_child()->get_statistics()->row_count();
        }
        row_percentage = 100 * row_count / input_count;
      } catch (...) {
        // Couldn't create statistics. Using default value of 100%
      }
    }


    std::ostringstream label_stream;
    if (!isnan(row_count)) {
      label_stream << std::fixed << std::setprecision(1) << row_count << " row(s) | " << row_percentage << "% estd.";
    } else {
      label_stream << "no est.";
    }

    VizEdgeInfo info;
    info.label = label_stream.str();
    info.pen_width = pen_width;

    _add_edge(from, to, info);
  }
};

}  // namespace opossum
