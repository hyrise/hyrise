#pragma once

#include <boost/algorithm/string.hpp>
#include <iomanip>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "logical_query_plan/abstract_lqp_node.hpp"
#include "optimizer/table_statistics.hpp"
#include "planviz/abstract_visualizer.hpp"

namespace opossum {

class LQPVisualizer : public AbstractVisualizer<std::vector<AbstractLQPNodeSPtr>> {
 public:
  LQPVisualizer();

  LQPVisualizer(GraphvizConfig graphviz_config, VizGraphInfo graph_info, VizVertexInfo vertex_info,
                VizEdgeInfo edge_info);

 protected:
  void _build_graph(const std::vector<AbstractLQPNodeSPtr>& lqp_roots) override;

  void _build_subtree(const AbstractLQPNodeSPtr& node,
                      std::unordered_set<AbstractLQPNodeCSPtr>& visualized_nodes);

  void _build_dataflow(const AbstractLQPNodeSPtr& from, const AbstractLQPNodeSPtr& to);
};

}  // namespace opossum
