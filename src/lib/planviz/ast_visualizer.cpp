#include "ast_visualizer.hpp"

#include <boost/algorithm/string.hpp>
#include <cmath>
#include <cstdlib>
#include <fstream>
#include <iomanip>
#include <memory>
#include <string>
#include <vector>

#include "dot_config.hpp"
#include "graphviz_tools.hpp"
#include "operators/abstract_operator.hpp"
#include "optimizer/table_statistics.hpp"
#include "sql/sql_query_plan.hpp"
#include "utils/assert.hpp"

namespace opossum {

ASTVisualizer::ASTVisualizer(const DotConfig& config)
    : _config(config) {}

void ASTVisualizer::visualize(const std::vector<std::shared_ptr<AbstractASTNode>>& ast_roots,
                              const std::string& output_prefix) {
  const auto dot_filename = output_prefix + ".dot";

  // Step 1: Generate graphviz dot file
  std::ofstream file;
  file.open(output_prefix + ".dot");
  file << "digraph {" << std::endl;
  file << "rankdir=BT" << std::endl;
  file << "bgcolor=" << dot_color_to_string.at(_config.background_color) << std::endl;
  file << "ratio=0.5" << std::endl;
  file << "node [color=white,fontcolor=white,shape=parallelogram]" << std::endl;
  file << "edge [color=white,fontcolor=white]" << std::endl;
  for (const auto& root : ast_roots) {
    visualize_subtree(root, file);
  }
  file << "}" << std::endl;
  file.close();

  // Step 2: Generate png from dot file
  graphviz_call_cmd(GraphvizLayout::Dot, _config.render_format, output_prefix);
}

void ASTVisualizer::visualize_subtree(const std::shared_ptr<AbstractASTNode>& node, std::ostream& stream) {
  auto already_visited = !_visited_subtrees.emplace(node).second;
  if (already_visited) {
    return;
  }

  stream << reinterpret_cast<uintptr_t>(node.get()) << "[label=\""
       << boost::replace_all_copy(node->description(DescriptionMode::MultiLine), "\"", "\\\"") << "\"]" << std::endl;

  if (node->left_child()) {
    visualize_dataflow(node->left_child(), node, stream);
    visualize_subtree(node->left_child(), stream);
  }

  if (node->right_child()) {
    visualize_dataflow(node->right_child(), node, stream);
    visualize_subtree(node->right_child(), stream);
  }
}

void ASTVisualizer::visualize_dataflow(const std::shared_ptr<AbstractASTNode>& from,
                                        const std::shared_ptr<AbstractASTNode>& to, std::ostream& stream) {
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

  stream << reinterpret_cast<uintptr_t>(from.get()) << " -> " << reinterpret_cast<uintptr_t>(to.get()) << "[label=\" ";
  if (!isnan(row_count)) {
    stream << std::fixed << std::setprecision(1) << row_count << " row(s) | " << row_percentage << "% estd.";
  } else {
    stream << "no est.";
  }
  stream << "\",penwidth=" << pen_width << "]" << std::endl;
}

}  // namespace opossum
