#include "tracking_memory_resource.hpp"

namespace opossum {

std::chrono::system_clock::time_point TrackingMemoryResource::_get_timestamp() const {
  return std::chrono::system_clock::now();
}

void* TrackingMemoryResource::do_allocate(std::size_t bytes, std::size_t alignment) {
  _memory_timeseries.emplace_back(std::make_pair(_get_timestamp(), bytes));
  return std::malloc(bytes);
}

void TrackingMemoryResource::do_deallocate(void* p, std::size_t bytes, std::size_t alignment) {
  std::free(p);
  _memory_timeseries.emplace_back(std::make_pair(_get_timestamp(), -1 * bytes));
}

[[nodiscard]] bool TrackingMemoryResource::do_is_equal(const memory_resource& other) const BOOST_NOEXCEPT {
  return &other == this;
}

const std::vector<std::pair<std::chrono::system_clock::time_point, int64_t>>&
TrackingMemoryResource::memory_timeseries() const {
  return _memory_timeseries;
}

}  // namespace opossum
