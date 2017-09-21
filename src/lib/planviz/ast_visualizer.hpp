#pragma once

#include <memory>
#include <string>
#include <vector>

#include "common.hpp"
#include "optimizer/abstract_syntax_tree/abstract_ast_node.hpp"

namespace opossum {

class ASTVisualizer {
 public:
  static const std::string png_filename;

  static void visualize(const std::vector<std::shared_ptr<AbstractASTNode>> &ast_roots);

 protected:
  static void _visualize_subtree(const std::shared_ptr<AbstractASTNode> &node, std::ofstream &file);
  static void _visualize_dataflow(const std::shared_ptr<AbstractASTNode> &from,
                                  const std::shared_ptr<AbstractASTNode> &to, std::ofstream &file);
};

}  // namespace opossum
