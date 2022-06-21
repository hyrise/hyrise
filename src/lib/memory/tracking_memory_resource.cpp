#include "tracking_memory_resource.hpp"

namespace opossum {

// explicitly initialize tracked memory to zero
TrackingMemoryResource::TrackingMemoryResource() : _tracked_memory{0} {};

void* TrackingMemoryResource::do_allocate(std::size_t bytes, std::size_t alignment) {
  _tracked_memory += bytes;
  return std::malloc(bytes);
}

void TrackingMemoryResource::do_deallocate(void* p, std::size_t bytes, std::size_t alignment) {
  std::free(p);
  _tracked_memory -= bytes;
}

[[nodiscard]] bool TrackingMemoryResource::do_is_equal(const memory_resource& other) const BOOST_NOEXCEPT {
  return &other == this;
}

size_t TrackingMemoryResource::get_amount() const {
  return static_cast<size_t>(_tracked_memory);
}

} // namespace opossum