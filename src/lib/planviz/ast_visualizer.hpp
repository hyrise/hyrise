#pragma once

#include <boost/algorithm/string.hpp>
#include <iomanip>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "optimizer/abstract_syntax_tree/abstract_ast_node.hpp"
#include "optimizer/table_statistics.hpp"
#include "planviz/abstract_visualizer.hpp"

namespace opossum {

class ASTVisualizer : public AbstractVisualizer<std::vector<std::shared_ptr<AbstractASTNode>>> {
 public:
  ASTVisualizer();

  ASTVisualizer(GraphvizConfig graphviz_config, VizGraphInfo graph_info, VizVertexInfo vertex_info,
                VizEdgeInfo edge_info);

 protected:
  void _build_graph(const std::vector<std::shared_ptr<AbstractASTNode>>& ast_roots) override;

  void _build_subtree(const std::shared_ptr<AbstractASTNode>& node);

  void _build_dataflow(const std::shared_ptr<AbstractASTNode>& from, const std::shared_ptr<AbstractASTNode>& to);
};

}  // namespace opossum
