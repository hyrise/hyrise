#include "lqp_visualizer.hpp"

#include <iomanip>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "expression/expression_utils.hpp"
#include "expression/lqp_subquery_expression.hpp"
#include "logical_query_plan/lqp_utils.hpp"
#include "logical_query_plan/projection_node.hpp"

namespace opossum {

LQPVisualizer::LQPVisualizer() {
  // Set defaults for this visualizer
  _default_vertex.shape = "rectangle";

  // We can guarantee the LQP never changes during visualization and thus avoid redundant estimations for subplans
  _cardinality_estimator.guarantee_bottom_up_construction();
}

LQPVisualizer::LQPVisualizer(GraphvizConfig graphviz_config, VizGraphInfo graph_info, VizVertexInfo vertex_info,
                             VizEdgeInfo edge_info)
    : AbstractVisualizer(std::move(graphviz_config), std::move(graph_info), std::move(vertex_info),
                         std::move(edge_info)) {}

void LQPVisualizer::_build_graph(const std::vector<std::shared_ptr<AbstractLQPNode>>& lqp_roots) {
  std::unordered_set<std::shared_ptr<const AbstractLQPNode>> visualized_nodes;
  ExpressionUnorderedSet visualized_sub_queries;

  for (const auto& root : lqp_roots) {
    _build_subtree(root, visualized_nodes, visualized_sub_queries);
  }
}

void LQPVisualizer::_build_subtree(const std::shared_ptr<AbstractLQPNode>& node,
                                   std::unordered_set<std::shared_ptr<const AbstractLQPNode>>& visualized_nodes,
                                   ExpressionUnorderedSet& visualized_sub_queries) {
  // Avoid drawing dataflows/ops redundantly in diamond shaped Nodes
  if (visualized_nodes.find(node) != visualized_nodes.end()) return;
  visualized_nodes.insert(node);

  auto node_label = node->description();
  if (!node->comment.empty()) {
    node_label += "\\n(" + node->comment + ")";
  }
  _add_vertex(node, node_label);

  if (node->left_input()) {
    auto left_input = node->left_input();
    _build_subtree(left_input, visualized_nodes, visualized_sub_queries);
    _build_dataflow(left_input, node, InputSide::Left);
  }

  if (node->right_input()) {
    auto right_input = node->right_input();
    _build_subtree(right_input, visualized_nodes, visualized_sub_queries);
    _build_dataflow(right_input, node, InputSide::Right);
  }

  // Visualize subqueries
  for (const auto& expression : node->node_expressions) {
    visit_expression(expression, [&](const auto& sub_expression) {
      const auto subquery_expression = std::dynamic_pointer_cast<LQPSubqueryExpression>(sub_expression);
      if (!subquery_expression) return ExpressionVisitation::VisitArguments;

      if (!visualized_sub_queries.emplace(subquery_expression).second) return ExpressionVisitation::VisitArguments;

      _build_subtree(subquery_expression->lqp, visualized_nodes, visualized_sub_queries);

      auto edge_info = _default_edge;
      auto correlated_str = std::string(subquery_expression->is_correlated() ? "correlated" : "uncorrelated");
      edge_info.label = correlated_str + " subquery";
      edge_info.style = "dashed";
      _add_edge(subquery_expression->lqp, node, edge_info);

      return ExpressionVisitation::VisitArguments;
    });
  }
}

void LQPVisualizer::_build_dataflow(const std::shared_ptr<AbstractLQPNode>& from,
                                    const std::shared_ptr<AbstractLQPNode>& to, const InputSide side) {
  float row_count = NAN;
  double pen_width = 1.0;
  auto row_percentage = 100.0f;

  try {
    row_count = _cardinality_estimator.estimate_cardinality(from);
    pen_width = row_count;
  } catch (...) {
    // statistics don't exist for this edge
  }

  if (from->left_input()) {
    try {
      float input_count = _cardinality_estimator.estimate_cardinality(from->left_input());

      // Include right side in cardinality estimation unless it is a semi/anti join
      const auto join_node = std::dynamic_pointer_cast<JoinNode>(from);
      if (from->right_input() &&
          (!join_node || (join_node->join_mode != JoinMode::Semi && join_node->join_mode != JoinMode::AntiNullAsTrue &&
                          join_node->join_mode != JoinMode::AntiNullAsFalse))) {
        input_count *= _cardinality_estimator.estimate_cardinality(from->right_input());
      }
      row_percentage = 100 * row_count / input_count;
    } catch (...) {
      // Couldn't create statistics. Using default value of 100%
    }
  }

  std::ostringstream label_stream;

  // Use a copy of the stream's default locale with thousands separators: Dynamically allocated raw pointers should
  // be avoided whenever possible. Unfortunately, std::locale stores pointers to the facets and does internal
  // reference counting. std::locale's destructor destructs the locale and the facets whose reference count becomes
  // zero. This forces us to use a dynamically allocated raw pointer here.
  const auto& separate_thousands_locale = std::locale(label_stream.getloc(), new SeparateThousandsFacet);
  label_stream.imbue(separate_thousands_locale);

  if (!isnan(row_count)) {
    label_stream << " " << std::fixed << std::setprecision(1) << row_count << " row(s) | " << row_percentage
                 << "% estd.";
  } else {
    label_stream << "no est.";
  }

  std::stringstream tooltip_stream;

  // Edge Tooltip: Node Output Expressions
  tooltip_stream << "Output Expressions: \n";
  const auto& output_expressions = from->output_expressions();
  for (auto column_id = ColumnID{0}; column_id < output_expressions.size(); ++column_id) {
    tooltip_stream << " (" << column_id + 1 << ") ";
    tooltip_stream << output_expressions.at(column_id)->as_column_name();
    if (from->is_column_nullable(column_id)) tooltip_stream << " NULL";
    tooltip_stream << "\n";
  }

  if (!dynamic_pointer_cast<AbstractNonQueryNode>(from)) {
    // Edge Tooltip: Unique Constraints
    const auto& unique_constraints = from->unique_constraints();
    tooltip_stream << "\n"
                   << "Unique Constraints: \n";
    if (unique_constraints->empty()) tooltip_stream << " <none>\n";
    for (auto uc_idx = size_t{0}; uc_idx < unique_constraints->size(); ++uc_idx) {
      tooltip_stream << " (" << uc_idx + 1 << ") ";
      tooltip_stream << unique_constraints->at(uc_idx) << "\n";
    }

    // Edge Tooltip: Trivial FDs
    auto trivial_fds = std::vector<FunctionalDependency>();
    if (!unique_constraints->empty()) trivial_fds = fds_from_unique_constraints(from, unique_constraints);
    tooltip_stream << "\n"
                   << "Functional Dependencies (trivial): \n";
    if (trivial_fds.empty()) tooltip_stream << " <none>\n";
    for (auto fd_idx = size_t{0}; fd_idx < trivial_fds.size(); ++fd_idx) {
      tooltip_stream << " (" << fd_idx + 1 << ") ";
      tooltip_stream << trivial_fds.at(fd_idx) << "\n";
    }

    // Edge Tooltip: Non-trivial FDs
    const auto& fds = from->non_trivial_functional_dependencies();
    tooltip_stream << "\n"
                   << "Functional Dependencies (non-trivial): \n";
    if (fds.empty()) tooltip_stream << " <none>";
    for (auto fd_idx = size_t{0}; fd_idx < fds.size(); ++fd_idx) {
      tooltip_stream << " (" << fd_idx + 1 << ") ";
      tooltip_stream << fds.at(fd_idx) << "\n";
    }
  }

  VizEdgeInfo info = _default_edge;
  info.label = label_stream.str();
  info.label_tooltip = tooltip_stream.str();
  info.pen_width = pen_width;
  if (to->input_count() == 2) {
    info.arrowhead = side == InputSide::Left ? "lnormal" : "rnormal";
  }

  _add_edge(from, to, info);
}

}  // namespace opossum
