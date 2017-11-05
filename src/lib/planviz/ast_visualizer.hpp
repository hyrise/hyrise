#pragma once

#include <memory>
#include <string>
#include <iostream>
#include <unordered_set>
#include <vector>

#include "dot_config.hpp"
#include "optimizer/abstract_syntax_tree/abstract_ast_node.hpp"

namespace opossum {

class ASTVisualizer final {
 public:
  ASTVisualizer(const DotConfig& config = {});

  void visualize(const std::vector<std::shared_ptr<AbstractASTNode>>& ast_roots, const std::string& output_prefix);

  void visualize_subtree(const std::shared_ptr<AbstractASTNode>& node, std::ostream& stream);
  void visualize_dataflow(const std::shared_ptr<AbstractASTNode>& from, const std::shared_ptr<AbstractASTNode>& to,
                           std::ostream& stream);

 private:
  const DotConfig _config;

  // Saving all subtrees we already processed makes sure we're not generating edge duplicates in multi-parent trees
  std::unordered_set<std::shared_ptr<AbstractASTNode>> _visited_subtrees;
};

}  // namespace opossum
