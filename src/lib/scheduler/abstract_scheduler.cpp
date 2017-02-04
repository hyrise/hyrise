#include "abstract_scheduler.hpp"

#include <memory>

#include "topology.hpp"

namespace opossum {

AbstractScheduler::AbstractScheduler(std::shared_ptr<Topology> topology) : _topology(topology) {}

const std::shared_ptr<Topology>& AbstractScheduler::topology() const { return _topology; }
}  // namespace opossum
