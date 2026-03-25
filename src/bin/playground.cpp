#include <iostream>

#include "magic_enum/magic_enum.hpp"

using namespace hyrise;  // NOLINT(build/namespaces)

enum class AccessType {
  Point /* Single point access */,
  Sequential /* 0, 1, 1, 2, 3, 4 */,
  Monotonic /* 0, 0, 1, 2, 4, 8, 17 */,
  Random /* 0, 1, 0, 42 */,
  Dictionary /* Used to count accesses to the dictionary of the dictionary segment */
};
using CounterType = std::atomic_uint64_t;

int main() {
  std::array<CounterType, magic_enum::enum_count<AccessType>()> _counters = {};
  std::cout << _counters.size() << "\n";

  return 0;
}
