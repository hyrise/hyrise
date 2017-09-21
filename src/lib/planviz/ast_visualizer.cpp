#include "ast_visualizer.hpp"

#include <cmath>
#include <cstdlib>
#include <fstream>
#include <iomanip>
#include <memory>
#include <string>
#include <vector>

#include "common.hpp"
#include "operators/abstract_operator.hpp"
#include "optimizer/table_statistics.hpp"
#include "sql/sql_query_plan.hpp"

namespace opossum {

const std::string ASTVisualizer::png_filename{"./.ast.png"};  // NOLINT

void ASTVisualizer::visualize(const std::vector<std::shared_ptr<AbstractASTNode>> &ast_roots) {
  // Step 1: Generate graphviz dot file
  std::ofstream file;
  file.open(".ast.dot");
  file << "digraph {" << std::endl;
  file << "rankdir=BT" << std::endl;
  file << "bgcolor=transparent" << std::endl;
  file << "node [color=white,fontcolor=white,shape=parallelogram]" << std::endl;
  file << "edge [color=white,fontcolor=white]" << std::endl;
  for (const auto &root : ast_roots) {
    _visualize_subtree(root, file);
  }
  file << "}" << std::endl;
  file.close();

  // Step 2: Generate png from dot file
  auto cmd = std::string("dot -Tpng .ast.dot > ") + png_filename;
  auto ret = system(cmd.c_str());

  Assert(ret == 0,
         "Calling graphviz' dot failed. Have you installed graphviz "
         "(apt-get install graphviz / brew install graphviz)?");
  // We do not want to make graphviz a requirement for Hyrise as visualization is just a gimmick
}

void ASTVisualizer::_visualize_subtree(const std::shared_ptr<AbstractASTNode> &node, std::ofstream &file) {
  file << reinterpret_cast<uintptr_t>(node.get()) << "[label=\"" << node->description() << "\"]" << std::endl;

  if (node->left_child()) {
    _visualize_dataflow(node->left_child(), node, file);
    _visualize_subtree(node->left_child(), file);
  }

  if (node->right_child()) {
    _visualize_dataflow(node->right_child(), node, file);
    _visualize_subtree(node->right_child(), file);
  }
}

void ASTVisualizer::_visualize_dataflow(const std::shared_ptr<AbstractASTNode> &from,
                                        const std::shared_ptr<AbstractASTNode> &to, std::ofstream &file) {
  float row_count, row_percentage = 100.0f;
  int penwidth;

  try {
    row_count = from->get_statistics()->row_count();
    penwidth = std::fmax(1, std::ceil(std::log10(row_count) / 2));
  } catch (...) {
    // statistics don't exist for this edge
    row_count = NAN;
    penwidth = 1;
  }

  if (from->left_child()) {
    try {
      float input_count = from->left_child()->get_statistics()->row_count();
      if (from->right_child()) {
        input_count *= from->right_child()->get_statistics()->row_count();
      }
      row_percentage = 100 * row_count / input_count;
    } catch (...) {
    }
  }

  file << reinterpret_cast<uintptr_t>(from.get()) << " -> " << reinterpret_cast<uintptr_t>(to.get()) << "[label=\" ";
  if (!isnan(row_count)) {
    file << std::setprecision(1)  // TODO make this work
         << std::to_string(row_count) << " row(s) | " << std::to_string(row_percentage) << "% estd.";
  } else {
    file << "no est.";
  }
  file << "\",penwidth=" << static_cast<int>(penwidth) << "]" << std::endl;
}

}  // namespace opossum
