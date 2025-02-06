#include <limits>
#include <cmath>
#include <type_traits>

namespace hyrise {

template <typename T>
bool floating_equal(const T a, const T b) {
  return std::abs(a - b) < std::numeric_limits<T>::epsilon();
}

template <typename T>
bool floating_container_equal(const T& a, const T& b) {
  const auto count = a.size();
  if (count != b.size()) {
    return false;
  }

  for (auto idx = size_t{0}; idx < count; ++idx) {
    if (a[idx] != b [idx]) {
      return false;
    }
  }

  return true;
}

}  // namespace hyrise
