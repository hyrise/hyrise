#pragma once

#include <vector>
#include <memory>

#include "scheduler/operator_task.hpp"

namespace tpcc {
using TaskVector = std::vector<std::shared_ptr<opossum::OperatorTask>>;
}