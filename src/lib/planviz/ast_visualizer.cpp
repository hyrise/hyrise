#include "ast_visualizer.hpp"

#include <boost/algorithm/string.hpp>
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

void ASTVisualizer::visualize(const std::vector<std::shared_ptr<AbstractASTNode>>& ast_roots,
                              const std::string& dot_filename, const std::string& img_filename) {
  // Step 1: Generate graphviz dot file
  std::ofstream file;
  file.open(dot_filename);
  file << "digraph {" << std::endl;
  file << "rankdir=BT" << std::endl;
  file << "bgcolor=transparent" << std::endl;
  file << "node [color=white,fontcolor=white,shape=parallelogram]" << std::endl;
  file << "edge [color=white,fontcolor=white]" << std::endl;
  for (const auto& root : ast_roots) {
    _visualize_subtree(root, file);
  }
  file << "}" << std::endl;
  file.close();

  // Step 2: Generate png from dot file
  auto cmd = std::string("dot -Tpng " + dot_filename + " > ") + img_filename;
  auto ret = system(cmd.c_str());

  Assert(ret == 0,
         "Calling graphviz' dot failed. Have you installed graphviz "
         "(apt-get install graphviz / brew install graphviz)?");
  // We do not want to make graphviz a requirement for Hyrise as visualization is just a gimmick
}

void ASTVisualizer::_visualize_subtree(const std::shared_ptr<AbstractASTNode>& node, std::ofstream& file) {
  file << reinterpret_cast<uintptr_t>(node.get()) << "[label=\""
       << boost::replace_all_copy(node->description(), "\"", "\\\"") << "\"]" << std::endl;

  if (node->left_child()) {
    _visualize_dataflow(node->left_child(), node, file);
    _visualize_subtree(node->left_child(), file);
  }

  if (node->right_child()) {
    _visualize_dataflow(node->right_child(), node, file);
    _visualize_subtree(node->right_child(), file);
  }
}

void ASTVisualizer::_visualize_dataflow(const std::shared_ptr<AbstractASTNode>& from,
                                        const std::shared_ptr<AbstractASTNode>& to, std::ofstream& file) {
  float row_count, row_percentage = 100.0f;
  uint32_t pen_width;

  try {
    row_count = from->get_statistics()->row_count();
    pen_width = std::fmax(1, std::ceil(std::log10(row_count) / 2));
  } catch (...) {
    // statistics don't exist for this edge
    row_count = NAN;
    pen_width = 1;
  }

  if (from->left_child()) {
    try {
      float input_count = from->left_child()->get_statistics()->row_count();
      if (from->right_child()) {
        input_count *= from->right_child()->get_statistics()->row_count();
      }
      row_percentage = 100 * row_count / input_count;
    } catch (...) {
      // Couldn't create statistics. Using default value of 100%
    }
  }

  file << reinterpret_cast<uintptr_t>(from.get()) << " -> " << reinterpret_cast<uintptr_t>(to.get()) << "[label=\" ";
  if (!isnan(row_count)) {
    file << std::fixed << std::setprecision(1) << row_count << " row(s) | " << row_percentage << "% estd.";
  } else {
    file << "no est.";
  }
  file << "\",penwidth=" << pen_width << "]" << std::endl;
}

}  // namespace opossum
