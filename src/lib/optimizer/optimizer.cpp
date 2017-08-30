#include "optimizer.hpp"

#include <memory>

namespace opossum {

std::shared_ptr<AbstractASTNode> Optimizer::optimize(std::shared_ptr<AbstractASTNode> input) { return input; }
}  // namespace opossum
