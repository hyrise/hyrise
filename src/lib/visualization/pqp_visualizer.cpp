#include <memory>
#include <string>
#include <utility>

#include "constant_mappings.hpp"
#include "expression/expression_utils.hpp"
#include "expression/pqp_subquery_expression.hpp"
#include "operators/limit.hpp"
#include "operators/projection.hpp"
#include "operators/table_scan.hpp"
#include "utils/format_bytes.hpp"
#include "utils/format_duration.hpp"
#include "visualization/abstract_visualizer.hpp"
#include "visualization/pqp_visualizer.hpp"

namespace opossum {

PQPVisualizer::PQPVisualizer() = default;

PQPVisualizer::PQPVisualizer(GraphvizConfig graphviz_config, VizGraphInfo graph_info, VizVertexInfo vertex_info,
                             VizEdgeInfo edge_info)
    : AbstractVisualizer(std::move(graphviz_config), std::move(graph_info), std::move(vertex_info),
                         std::move(edge_info)) {}

void PQPVisualizer::_build_graph(const std::vector<std::shared_ptr<AbstractOperator>>& plans) {
  std::unordered_set<std::shared_ptr<const AbstractOperator>> visualized_ops;

  for (const auto& plan : plans) {
    _build_subtree(plan, visualized_ops);
  }

  {
    // Print the "Total by operator" box using graphviz's record type. Using HTML labels would be slightly nicer, but
    // boost always encloses the label in quotes, which breaks them.
    std::stringstream operator_breakdown_stream;
    operator_breakdown_stream << "{Total by operator|{";

    auto sorted_duration_by_operator_name = std::vector<std::pair<std::string, std::chrono::nanoseconds>>{
        _duration_by_operator_name.begin(), _duration_by_operator_name.end()};
    std::sort(sorted_duration_by_operator_name.begin(), sorted_duration_by_operator_name.end(),
              [](const auto& lhs, const auto& rhs) { return lhs.second.count() > rhs.second.count(); });

    // Print first column (operator name)
    for (const auto& [operator_name, _] : sorted_duration_by_operator_name) {
      operator_breakdown_stream << " " << operator_name << " \\r";
    }
    operator_breakdown_stream << "total\\r";

    // Print second column (operator duration) and track total duration
    operator_breakdown_stream << "|";
    auto total_nanoseconds = std::chrono::nanoseconds{};
    for (const auto& [_, nanoseconds] : sorted_duration_by_operator_name) {
      operator_breakdown_stream << " " << format_duration(nanoseconds) << " \\l";
      total_nanoseconds += nanoseconds;
    }
    operator_breakdown_stream << " " << format_duration(total_nanoseconds) << " \\l";

    // Print third column (relative operator duration)
    operator_breakdown_stream << "|";
    for (const auto& [_, nanoseconds] : sorted_duration_by_operator_name) {
      operator_breakdown_stream << round(static_cast<double>(nanoseconds.count()) /
                                         static_cast<double>(total_nanoseconds.count()) * 100)
                                << " %\\l";
    }
    operator_breakdown_stream << " \\l";

    operator_breakdown_stream << "}}";

    VizVertexInfo vertex_info = _default_vertex;
    vertex_info.shape = "record";
    vertex_info.label = operator_breakdown_stream.str();

    boost::add_vertex(vertex_info, _graph);
  }
}

void PQPVisualizer::_build_subtree(const std::shared_ptr<const AbstractOperator>& op,
                                   std::unordered_set<std::shared_ptr<const AbstractOperator>>& visualized_ops) {
  // Avoid drawing dataflows/ops redundantly in diamond shaped PQPs
  if (visualized_ops.find(op) != visualized_ops.end()) return;
  visualized_ops.insert(op);

  _add_operator(op);

  if (op->left_input()) {
    auto left = op->left_input();
    _build_subtree(left, visualized_ops);
    _build_dataflow(left, op, InputSide::Left);
  }

  if (op->right_input()) {
    auto right = op->right_input();
    _build_subtree(right, visualized_ops);
    _build_dataflow(right, op, InputSide::Right);
  }

  switch (op->type()) {
    case OperatorType::Projection: {
      const auto projection = std::dynamic_pointer_cast<const Projection>(op);
      for (const auto& expression : projection->expressions) {
        _visualize_subqueries(op, expression, visualized_ops);
      }
    } break;

    case OperatorType::TableScan: {
      const auto table_scan = std::dynamic_pointer_cast<const TableScan>(op);
      _visualize_subqueries(op, table_scan->predicate(), visualized_ops);
    } break;

    case OperatorType::Limit: {
      const auto limit = std::dynamic_pointer_cast<const Limit>(op);
      _visualize_subqueries(op, limit->row_count_expression(), visualized_ops);
    } break;

    default: {
    }  // OperatorType has no expressions
  }
}

void PQPVisualizer::_visualize_subqueries(const std::shared_ptr<const AbstractOperator>& op,
                                          const std::shared_ptr<AbstractExpression>& expression,
                                          std::unordered_set<std::shared_ptr<const AbstractOperator>>& visualized_ops) {
  visit_expression(expression, [&](const auto& sub_expression) {
    const auto pqp_subquery_expression = std::dynamic_pointer_cast<PQPSubqueryExpression>(sub_expression);
    if (!pqp_subquery_expression) return ExpressionVisitation::VisitArguments;

    _build_subtree(pqp_subquery_expression->pqp, visualized_ops);

    auto edge_info = _default_edge;
    auto correlated_str = std::string(pqp_subquery_expression->is_correlated() ? "correlated" : "uncorrelated");
    edge_info.label = correlated_str + " subquery";
    edge_info.style = "dashed";
    _add_edge(pqp_subquery_expression->pqp, op, edge_info);

    return ExpressionVisitation::VisitArguments;
  });
}

void PQPVisualizer::_build_dataflow(const std::shared_ptr<const AbstractOperator>& from,
                                    const std::shared_ptr<const AbstractOperator>& to, const InputSide side) {
  VizEdgeInfo info = _default_edge;

  const auto& performance_data = *from->performance_data;
  if (performance_data.executed && performance_data.has_output) {
    std::stringstream stream;

    // Use a copy of the stream's default locale with thousands separators: Dynamically allocated raw pointers should
    // be avoided whenever possible. Unfortunately, std::locale stores pointers to the facets and does internal
    // reference counting. std::locale's destructor destructs the locale and the facets whose reference count becomes
    // zero. This forces us to use a dynamically allocated raw pointer here.
    const auto& separate_thousands_locale = std::locale(stream.getloc(), new SeparateThousandsFacet);
    stream.imbue(separate_thousands_locale);

    stream << performance_data.output_row_count << " row(s)/";
    stream << performance_data.output_chunk_count << " chunk(s)";
    info.label = stream.str();
  }

  info.pen_width = static_cast<double>(performance_data.output_row_count);
  if (to->right_input() != nullptr) {
    info.arrowhead = side == InputSide::Left ? "lnormal" : "rnormal";
  }

  _add_edge(from, to, info);
}

void PQPVisualizer::_add_operator(const std::shared_ptr<const AbstractOperator>& op) {
  VizVertexInfo info = _default_vertex;
  auto label = op->description(DescriptionMode::MultiLine);

  const auto& performance_data = *op->performance_data;
  if (performance_data.executed) {
    auto total = performance_data.walltime;
    label += "\n\n" + format_duration(total);
    info.pen_width = static_cast<double>(total.count());

    std::stringstream operator_performance_data_stream;
    performance_data.output_to_stream(operator_performance_data_stream, DescriptionMode::MultiLine);
    info.tooltip = operator_performance_data_stream.str();
  } else {
    info.pen_width = 1.0;
  }

  _duration_by_operator_name[op->name()] += performance_data.walltime;

  info.label = label;
  _add_vertex(op, info);
}

}  // namespace opossum
