#pragma once

#include <memory>
#include <string>
#include <vector>

#include "logical_query_plan/abstract_logical_query_plan_node.hpp"

namespace opossum {

class ASTVisualizer {
 public:
  static void visualize(const std::vector<std::shared_ptr<AbstractLogicalQueryPlanNode>>& ast_roots, const std::string& dot_filename,
                        const std::string& img_filename);

 protected:
  static void _visualize_subtree(const std::shared_ptr<AbstractLogicalQueryPlanNode>& node, std::ofstream& file);
  static void _visualize_dataflow(const std::shared_ptr<AbstractLogicalQueryPlanNode>& from,
                                  const std::shared_ptr<AbstractLogicalQueryPlanNode>& to, std::ofstream& file);
};

}  // namespace opossum
