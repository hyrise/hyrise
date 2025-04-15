#include "default_memory_resource.hpp"

#include <cstddef>
#include <cstdlib>

namespace hyrise {

void* DefaultResource::do_allocate(std::size_t bytes, std::size_t /*alignment*/) {
  return std::malloc(bytes);
}

void DefaultResource::do_deallocate(void* pointer, std::size_t /*bytes*/, std::size_t /*alignment*/) {
  std::free(pointer);
}

[[nodiscard]] bool DefaultResource::do_is_equal(const memory_resource& other) const noexcept {
  return &other == this;
}

}  // namespace hyrise
