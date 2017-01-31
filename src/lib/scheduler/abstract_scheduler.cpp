#include <memory>

#include "abstract_scheduler.hpp"
#include "abstract_topology.hpp"

namespace opossum {

AbstractScheduler::AbstractScheduler(std::shared_ptr<AbstractTopology> topology) : _topology(topology) {}

const std::shared_ptr<AbstractTopology>& AbstractScheduler::topology() const { return _topology; }
}
