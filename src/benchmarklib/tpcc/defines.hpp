#pragma once

#include <memory>
#include <vector>

namespace opossum {

class OperatorTask;

}  // namespace opossum

namespace tpcc {

using TaskVector = std::vector<std::shared_ptr<opossum::OperatorTask>>;

}  // namespace tpcc
