#include "threading_utils.hpp"

#include <pthread.h>
#include <sched.h>

#include "types.hpp"

namespace hyrise {

void SetThreadAffinity(const CpuID cpu_id) {
#if HYRISE_NUMA_SUPPORT
  auto cpuset = cpu_set_t{};
  CPU_ZERO(&cpuset);
  CPU_SET(cpu_id, &cpuset);
  const auto return_code = pthread_setaffinity_np(pthread_self(), sizeof(cpu_set_t), &cpuset);
  Assert(return_code == 0, "Error calling pthread_setaffinity_np (return code: " + std::to_string(return_code) + ").");
#endif
}

}  // namespace hyrise
