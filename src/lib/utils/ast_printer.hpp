#pragma once

#include <cstdint>
#include <iostream>
#include <memory>
#include <vector>

namespace opossum {

class AbstractASTNode;

/**
 * Console friendly printing of ASTs
 */
class ASTPrinter {
 public:
  static void print(const std::shared_ptr<AbstractASTNode> & node,
                    std::vector<bool> levels = {},
                    std::ostream& out = std::cout);
};

}