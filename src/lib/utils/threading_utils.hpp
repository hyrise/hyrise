#pragma once

#include "types.hpp"

namespace hyrise {

// Pin a worker to a particular core. As of now, this does not work on non-NUMA systems.
void SetThreadAffinity(const CpuID cpu_id);
void UnsetThreadAffinity();

}  // namespace hyrise
