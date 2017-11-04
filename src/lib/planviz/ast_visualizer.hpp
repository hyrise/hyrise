#pragma once

#include <memory>
#include <string>
#include <unordered_set>
#include <vector>

#include "optimizer/abstract_syntax_tree/abstract_ast_node.hpp"
#include "dot_config.hpp"

namespace opossum {

class ASTVisualizer final {
 public:
  ASTVisualizer(const std::vector<std::shared_ptr<AbstractASTNode>>& ast_roots, const std::string& output_prefix, DotConfig config = {});

  void visualize();

 protected:
  void _visualize_subtree(const std::shared_ptr<AbstractASTNode>& node, std::ofstream& file);
  void _visualize_dataflow(const std::shared_ptr<AbstractASTNode>& from,
                                  const std::shared_ptr<AbstractASTNode>& to, std::ofstream& file);

 private:
  const std::vector<std::shared_ptr<AbstractASTNode>> _ast_roots;
  const std::string _output_prefix;
  const DotConfig _config;

  // Saving all subtrees we already processed makes sure we're not generating edge duplicates in multi-parent trees
  std::unordered_set<std::shared_ptr<AbstractASTNode>> _visited_subtrees;
};

}  // namespace opossum
