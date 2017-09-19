#include "sql_query_plan_visualizer.hpp"

#include <cstdlib>
#include <fstream>
#include <memory>
#include <string>

#include "common.hpp"
#include "operators/abstract_operator.hpp"
#include "sql/sql_query_plan.hpp"

namespace opossum {

const std::string SQLQueryPlanVisualizer::png_filename{"./.queryplan.png"};  // NOLINT

void SQLQueryPlanVisualizer::visualize(SQLQueryPlan &plan) {
  // Step 1: Generate graphviz dot file
  std::ofstream file;
  file.open(".queryplan.dot");
  file << "digraph {" << std::endl;
  file << "rankdir=BT" << std::endl;
  file << "bgcolor=transparent" << std::endl;
  file << "node [color=white,fontcolor=white]" << std::endl;
  file << "edge [color=white,fontcolor=white]" << std::endl;
  for (const auto &root : plan.tree_roots()) {
    _visualize_subtree(root, file);
  }
  file << "}" << std::endl;
  file.close();

  // Step 2: Generate png from dot file
  auto cmd = std::string("dot -Tpng:quartz:quartz .queryplan.dot > ") + png_filename;
  auto ret = system(cmd.c_str());
  Assert(ret == 0, "calling graphviz' dot failed");
}

void SQLQueryPlanVisualizer::_visualize_subtree(const std::shared_ptr<const AbstractOperator> &op,
                                                std::ofstream &file) {
  switch (op->num_in_tables()) {
    case 2:
      file << reinterpret_cast<uintptr_t>(op->input_right().get()) << " -> " << reinterpret_cast<uintptr_t>(op.get())
           << std::endl;
      _visualize_subtree(op->input_right(), file);
#if __has_cpp_attribute(fallthrough)
      [[fallthrough]];
#endif
    case 1:
      file << reinterpret_cast<uintptr_t>(op->input_left().get()) << " -> " << reinterpret_cast<uintptr_t>(op.get())
           << std::endl;
      _visualize_subtree(op->input_left(), file);
#if __has_cpp_attribute(fallthrough)
      [[fallthrough]];
#endif
    case 0:
      file << reinterpret_cast<uintptr_t>(op.get()) << "[label=\"" << op->description() << "\"]" << std::endl;
      break;
  }
}

}  // namespace opossum
