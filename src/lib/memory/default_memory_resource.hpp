#pragma once

#include "types.hpp"
#include "utils/singleton.hpp"

namespace hyrise {

class DefaultResource : public MemoryResource, public Singleton<DefaultResource> {
 public:
  void* do_allocate(std::size_t bytes, std::size_t /*alignment*/) override;
  void do_deallocate(void* pointer, std::size_t /*bytes*/, std::size_t /*alignment*/) override;
  [[nodiscard]] bool do_is_equal(const MemoryResource& other) const noexcept override;
};

}  // namespace hyrise
