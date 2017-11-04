#pragma once

#include <memory>
#include <string>
#include <vector>

#include "dot_config.hpp"

namespace opossum {

class AbstractASTNode;

class JoinGraphVisualizer {
 public:
  JoinGraphVisualizer(const std::vector<std::shared_ptr<AbstractASTNode>>& ast_roots, const std::string& output_prefix,
                      const DotConfig& config = {});

  void visualize();

 private:
  const std::vector<std::shared_ptr<AbstractASTNode>> _ast_roots;
  const std::string _output_prefix;
  const DotConfig _config;

};

}
