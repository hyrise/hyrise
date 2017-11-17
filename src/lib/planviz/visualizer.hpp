#pragma once

#include <boost/graph/graphviz.hpp>

#include <fstream>
#include <vector>

#include "utils/assert.hpp"


namespace opossum {

template <typename T, typename Functor>
static void visualize_tree(const std::vector<T>& roots, const std::string& dot_filename,
                           const std::string& img_filename, const std::string& shape, Functor visualize_subtree) {
  std::ofstream file;
  file.open(dot_filename);
  file << "digraph {" << std::endl;
  file << "rankdir=BT" << std::endl;
  file << "bgcolor=transparent" << std::endl;
  file << "ratio=0.5" << std::endl;
  file << "node [color=black,fontcolor=black,shape=" << shape << "]" << std::endl;
  file << "edge [color=black,fontcolor=black]" << std::endl;

  for (const auto& root : roots) {
    visualize_subtree(root, file);
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
};

}  // namespace opossum
