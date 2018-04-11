#pragma once

#include <memory>
#include <vector>

namespace opossum {

class OperatorTask;

using TaskVector = std::vector<std::shared_ptr<OperatorTask>>;

}  // namespace opossum
