#include "ast_printer.hpp"

#include "optimizer/abstract_syntax_tree/abstract_ast_node.hpp"

namespace opossum {

void ASTPrinter::print(const std::shared_ptr<AbstractASTNode>& node, std::vector<bool> levels, std::ostream& out) {
  const auto max_level = levels.empty() ? 0 : levels.size() - 1;
  for (size_t level = 0; level < max_level; ++level) {
    if (levels[level]) {
      out << " | ";
    } else {
      out << "   ";
    }
  }

  if (!levels.empty()) {
    out << " \\_";
  }
  out << node->description() << std::endl;

  levels.emplace_back(node->right_child() != nullptr);

  if (node->left_child()) {
    print(node->left_child(), levels, out);
  }
  if (node->right_child()) {
    levels.back() = false;
    print(node->right_child(), levels, out);
  }

  levels.pop_back();
}
}