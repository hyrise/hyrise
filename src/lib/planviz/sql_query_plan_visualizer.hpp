#pragma once

#include <memory>
#include <string>
#include <utility>

#include "expression/abstract_expression.hpp"
#include "planviz/abstract_visualizer.hpp"
#include "sql/sql_query_plan.hpp"

namespace opossum {

class SQLQueryPlanVisualizer : public AbstractVisualizer<SQLQueryPlan> {
 public:
  SQLQueryPlanVisualizer();

  SQLQueryPlanVisualizer(GraphvizConfig graphviz_config, VizGraphInfo graph_info = {}, VizVertexInfo vertex_info = {},
                         VizEdgeInfo edge_info = {});

 protected:
  void _build_graph(const SQLQueryPlan& plan) override;

  void _build_subtree(const std::shared_ptr<const AbstractOperator>& op,
                      std::unordered_set<std::shared_ptr<const AbstractOperator>>& visualized_ops);

  void _visualize_subselects(const std::shared_ptr<const AbstractOperator>& op,
                             const std::shared_ptr<AbstractExpression>& expression,
                             std::unordered_set<std::shared_ptr<const AbstractOperator>>& visualized_ops);

  void _build_dataflow(const std::shared_ptr<const AbstractOperator>& from,
                       const std::shared_ptr<const AbstractOperator>& to);

  void _add_operator(const std::shared_ptr<const AbstractOperator>& op);
};

}  // namespace opossum
