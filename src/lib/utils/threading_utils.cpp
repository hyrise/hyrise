#include "threading_utils.hpp"

#if HYRISE_NUMA_SUPPORT
#include <pthread.h>
#include <sched.h>
#endif

#include <bitset>

#include "types.hpp"

namespace {

#if HYRISE_NUMA_SUPPORT
#endif

}  // namespace

namespace hyrise {

void SetThreadAffinity(const CpuID cpu_id) {
#if HYRISE_NUMA_SUPPORT
  auto cpuset = cpu_set_t{};
  CPU_ZERO(&cpuset);
  CPU_SET(cpu_id, &cpuset);
  //std::cout << "mask: " << std::bitset<64>(cpuset) << std::endl;
  const auto return_code = pthread_setaffinity_np(pthread_self(), sizeof(cpu_set_t), &cpuset);
  Assert(return_code == 0, "Error calling pthread_setaffinity_np (return code: " + std::to_string(return_code) + ").");
#endif
}

void UnsetThreadAffinity() {
#if HYRISE_NUMA_SUPPORT
  auto cpuset = cpu_set_t{};
  /*
  auto a = int64_t{1 << 32};
  --a;
  std::cout << "mask: " << std::bitset<64>(static_cast<int64_t>(a)) << std::endl;
  auto b = static_cast<cpu_set_t>(a);
  */
  for (int i = 0; i < 1'024; ++i) {
    CPU_SET(i, &cpuset);
  }
  const auto return_code = pthread_setaffinity_np(pthread_self(), sizeof(cpu_set_t), &cpuset);
  Assert(return_code == 0, "Error calling pthread_setaffinity_np (return code: " + std::to_string(return_code) + ").");
#endif
}

}  // namespace hyrise
