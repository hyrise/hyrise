#pragma once

#include <memory>
#include <string>
#include <vector>

#include "logical_query_plan/abstract_lqp_node.hpp"

namespace opossum {

class LQPVisualizer {
 public:
  static void visualize(const std::vector<std::shared_ptr<AbstractLQPNode>>& lqp_roots, const std::string& dot_filename,
                        const std::string& img_filename);

 protected:
  static void _visualize_subtree(const std::shared_ptr<AbstractLQPNode>& node, std::ofstream& file);
  static void _visualize_dataflow(const std::shared_ptr<AbstractLQPNode>& from,
                                  const std::shared_ptr<AbstractLQPNode>& to, std::ofstream& file);
};

}  // namespace opossum
