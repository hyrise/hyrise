#include "abstract_scheduler.hpp"

#include <memory>

namespace opossum {

AbstractScheduler::AbstractScheduler(TopologySPtr topology) : _topology(topology) {}

const TopologySPtr& AbstractScheduler::topology() const { return _topology; }
}  // namespace opossum
