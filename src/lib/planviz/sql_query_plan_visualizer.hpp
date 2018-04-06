#pragma once

#include <memory>
#include <string>
#include <utility>

#include "planviz/abstract_visualizer.hpp"
#include "sql/sql_query_plan.hpp"

namespace opossum {

class SQLQueryPlanVisualizer : public AbstractVisualizer<SQLQueryPlan> {
 public:
  SQLQueryPlanVisualizer();

  SQLQueryPlanVisualizer(GraphvizConfig graphviz_config, VizGraphInfo graph_info, VizVertexInfo vertex_info,
                         VizEdgeInfo edge_info);

 protected:
  void _build_graph(const SQLQueryPlan& plan) override;

  void _build_subtree(const AbstractOperatorCSPtr& op,
                      std::unordered_set<AbstractOperatorCSPtr>& visualized_ops);

  void _build_dataflow(const AbstractOperatorCSPtr& from,
                       const AbstractOperatorCSPtr& to);

  void _add_operator(const AbstractOperatorCSPtr& op);
};

}  // namespace opossum
