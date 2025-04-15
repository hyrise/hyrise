#pragma once

#include "types.hpp"
#include "utils/singleton.hpp"

namespace hyrise {

// We discourage manual memory management in Hyrise (such as malloc, or new), but in case of allocator/memory resource
// implementations, it is fine.

class DefaultResource : public MemoryResource, public Singleton<DefaultResource> {
 public:
  void* do_allocate(std::size_t bytes, std::size_t /*alignment*/) override;
  void do_deallocate(void* pointer, std::size_t /*bytes*/, std::size_t /*alignment*/) override;
  [[nodiscard]] bool do_is_equal(const memory_resource& other) const noexcept override;
};

}  // namespace hyrise
