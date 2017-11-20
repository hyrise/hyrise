#pragma once

#include <memory>
#include <string>
#include <utility>

#include "planviz/abstract_visualizer.hpp"
#include "planviz/sql_query_plan_visualizer.hpp"
#include "sql/sql_query_plan.hpp"

namespace opossum {

SQLQueryPlanVisualizer::SQLQueryPlanVisualizer() : AbstractVisualizer() {
  _default_vertex.font_color = GraphvizColor::Black;
  _default_vertex.color = GraphvizColor::Black;
  _default_edge.font_color = GraphvizColor::Black;
  _default_edge.color = GraphvizColor::Black;
}

SQLQueryPlanVisualizer::SQLQueryPlanVisualizer(GraphvizConfig graphviz_config, VizGraphInfo graph_info,
                                               VizVertexInfo vertex_info, VizEdgeInfo edge_info)
    : AbstractVisualizer(std::move(graphviz_config), std::move(graph_info), std::move(vertex_info),
                         std::move(edge_info)) {}

void SQLQueryPlanVisualizer::_build_graph(const SQLQueryPlan& plan) override {
  for (const auto& root : plan.tree_roots()) {
    _add_operator(root);
    _build_subtree(root);
  }
}

void SQLQueryPlanVisualizer::_build_subtree(const std::shared_ptr<const AbstractOperator>& op) {
  _add_operator(op);

  if (op->input_left() != nullptr) {
    auto left = op->input_left();
    _add_operator(left);
    _build_dataflow(left, op);
    _build_subtree(left);
  }

  if (op->input_right() != nullptr) {
    auto right = op->input_right();
    _add_operator(right);
    _build_dataflow(right, op);
    _build_subtree(right);
  }
}

void SQLQueryPlanVisualizer::_build_dataflow(const std::shared_ptr<const AbstractOperator>& from,
                                             const std::shared_ptr<const AbstractOperator>& to) {
  VizEdgeInfo info = _default_edge;

  if (const auto& output = from->get_output()) {
    // the input operator was executed, print the number of rows
    info.label = std::to_string(output->row_count()) + " row(s)";
    info.pen_width = static_cast<uint8_t>(std::fmax(1, std::ceil(std::log10(output->row_count()) / 2)));
  }

  _add_edge(from, to, std::move(info));
}

void SQLQueryPlanVisualizer::_add_operator(const std::shared_ptr<const AbstractOperator>& op) {
  VizVertexInfo info = _default_vertex;
  auto label = op->description();

  if (op->get_output()) {
    auto wall_time = op->performance_data().walltime_ns;
    label += "\\n" + std::to_string(wall_time) + " ns";
    info.pen_width = static_cast<uint8_t>(std::fmax(1, std::ceil(std::log10(wall_time) / 2)));
  }

  info.label = label;
  _add_vertex(op, info);
}

}  // namespace opossum
