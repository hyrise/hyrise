#pragma once

#include <memory>
#include <vector>

namespace hyrise {

class OperatorTask;

using TaskVector = std::vector<std::shared_ptr<OperatorTask>>;

}  // namespace hyrise
