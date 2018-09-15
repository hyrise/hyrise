#include <memory>
#include <string>
#include <utility>

#include "expression/expression_utils.hpp"
#include "expression/pqp_select_expression.hpp"
#include "operators/projection.hpp"
#include "planviz/abstract_visualizer.hpp"
#include "planviz/sql_query_plan_visualizer.hpp"
#include "sql/sql_query_plan.hpp"
#include "utils/format_duration.hpp"

namespace opossum {

SQLQueryPlanVisualizer::SQLQueryPlanVisualizer() = default;

SQLQueryPlanVisualizer::SQLQueryPlanVisualizer(GraphvizConfig graphviz_config, VizGraphInfo graph_info,
                                               VizVertexInfo vertex_info, VizEdgeInfo edge_info)
    : AbstractVisualizer(std::move(graphviz_config), std::move(graph_info), std::move(vertex_info),
                         std::move(edge_info)) {}

void SQLQueryPlanVisualizer::_build_graph(const SQLQueryPlan& plan) {
  std::unordered_set<std::shared_ptr<const AbstractOperator>> visualized_ops;

  for (const auto& root : plan.tree_roots()) {
    _build_subtree(root, visualized_ops);
  }
}

void SQLQueryPlanVisualizer::_build_subtree(
    const std::shared_ptr<const AbstractOperator>& op,
    std::unordered_set<std::shared_ptr<const AbstractOperator>>& visualized_ops) {
  // Avoid drawing dataflows/ops redundantly in diamond shaped PQPs
  if (visualized_ops.find(op) != visualized_ops.end()) return;
  visualized_ops.insert(op);

  _add_operator(op);

  if (op->input_left() != nullptr) {
    auto left = op->input_left();
    _build_subtree(left, visualized_ops);
    _build_dataflow(left, op);
  }

  if (op->input_right() != nullptr) {
    auto right = op->input_right();
    _build_subtree(right, visualized_ops);
    _build_dataflow(right, op);
  }

  // Visualize subselects
  if (const auto projection = std::dynamic_pointer_cast<const Projection>(op)) {
    for (const auto& column_expression : projection->expressions) {
      visit_expression(column_expression, [&](const auto& sub_expression) {
        const auto pqp_select_expression = std::dynamic_pointer_cast<PQPSelectExpression>(sub_expression);
        if (!pqp_select_expression) return ExpressionVisitation::VisitArguments;

        _build_subtree(pqp_select_expression->pqp, visualized_ops);

        auto edge_info = _default_edge;
        auto correlated_str = std::string(pqp_select_expression->is_correlated() ? "correlated" : "uncorrelated");
        edge_info.label = correlated_str + " subquery";
        edge_info.style = "dashed";
        _add_edge(pqp_select_expression->pqp, op, edge_info);

        return ExpressionVisitation::VisitArguments;
      });
    }
  }
}

void SQLQueryPlanVisualizer::_build_dataflow(const std::shared_ptr<const AbstractOperator>& from,
                                             const std::shared_ptr<const AbstractOperator>& to) {
  VizEdgeInfo info = _default_edge;

  if (const auto& output = from->get_output()) {
    std::stringstream stream;

    stream << std::to_string(output->row_count()) + " row(s)/";
    stream << std::to_string(output->chunk_count()) + " chunk(s)/";
    stream << format_bytes(output->estimate_memory_usage());

    info.label = stream.str();

    info.pen_width = std::fmax(1, std::ceil(std::log10(output->row_count()) / 2));
  }

  _add_edge(from, to, info);
}

void SQLQueryPlanVisualizer::_add_operator(const std::shared_ptr<const AbstractOperator>& op) {
  VizVertexInfo info = _default_vertex;
  auto label = op->description(DescriptionMode::MultiLine);

  if (op->get_output()) {
    auto total = op->performance_data().walltime;
    label += "\n\n" + format_duration(std::chrono::duration_cast<std::chrono::nanoseconds>(total));
    info.pen_width = std::fmax(1, std::ceil(std::log10(total.count()) / 2));
  }

  info.label = label;
  _add_vertex(op, info);
}

}  // namespace opossum
