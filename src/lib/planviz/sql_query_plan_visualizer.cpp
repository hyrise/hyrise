#include "sql_query_plan_visualizer.hpp"

#include <cmath>
#include <cstdlib>
#include <fstream>
#include <memory>
#include <string>

#include "common.hpp"
#include "operators/abstract_operator.hpp"
#include "sql/sql_query_plan.hpp"

namespace opossum {

const std::string SQLQueryPlanVisualizer::png_filename{"./.queryplan.png"};  // NOLINT

void SQLQueryPlanVisualizer::visualize(const SQLQueryPlan &plan) {
  // Step 1: Generate graphviz dot file
  std::ofstream file;
  file.open(".queryplan.dot");
  file << "digraph {" << std::endl;
  file << "rankdir=BT" << std::endl;
  file << "bgcolor=transparent" << std::endl;
  file << "node [color=white,fontcolor=white,shape=rectangle]" << std::endl;
  file << "edge [color=white,fontcolor=white]" << std::endl;
  for (const auto &root : plan.tree_roots()) {
    _visualize_subtree(root, file);
  }
  file << "}" << std::endl;
  file.close();

  // Step 2: Generate png from dot file
  auto cmd = std::string("dot -Tpng .queryplan.dot > ") + png_filename;
  auto ret = system(cmd.c_str());

  Assert(ret == 0,
         "Calling graphviz' dot failed. Have you installed graphviz "
         "(apt-get install graphviz / brew install graphviz)?");
  // We do not want to make graphviz a requirement for Hyrise as visualization is just a gimmick
}

void SQLQueryPlanVisualizer::_visualize_subtree(const std::shared_ptr<const AbstractOperator> &op,
                                                std::ofstream &file) {
  file << reinterpret_cast<uintptr_t>(op.get()) << "[label=\"" << op->description();

  if (op->get_output()) {
    file << "\\n"
         << op->performance_data().walltime_ns << " ns\",penwidth="
         << static_cast<int>(std::fmax(1, std::ceil(std::log10(op->performance_data().walltime_ns) / 2)));
  } else {
    file << "\"";
  }
  file << "]" << std::endl;

  if (op->num_in_tables() >= 1) {
    _visualize_dataflow(op->input_left(), op, file);
    _visualize_subtree(op->input_left(), file);
  }

  if (op->num_in_tables() == 2) {
    _visualize_dataflow(op->input_right(), op, file);
    _visualize_subtree(op->input_right(), file);
  }
}

void SQLQueryPlanVisualizer::_visualize_dataflow(const std::shared_ptr<const AbstractOperator> &from,
                                                 const std::shared_ptr<const AbstractOperator> &to,
                                                 std::ofstream &file) {
  file << reinterpret_cast<uintptr_t>(from.get()) << " -> " << reinterpret_cast<uintptr_t>(to.get());

  if (const auto &output = from->get_output()) {
    // the input operator was executed, print the number of rows
    file << "[label=\" " << std::to_string(output->row_count()) << " row(s)\""
         << ",penwidth=" << static_cast<int>(std::fmax(1, std::ceil(std::log10(output->row_count()) / 2))) << "]";
  }

  file << std::endl;
}

}  // namespace opossum
