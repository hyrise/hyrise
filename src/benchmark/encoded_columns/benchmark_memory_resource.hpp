#pragma once

#include "utils/numa_memory_resource.hpp"

namespace opossum {

class BenchmarkMemoryResource : public NUMAMemoryResource {
 public:
  BenchmarkMemoryResource(int node_id);

  void* do_allocate(std::size_t bytes, std::size_t alignment) override;
  void do_deallocate(void* p, std::size_t bytes, std::size_t alignment) override;
  bool do_is_equal(const memory_resource& other) const noexcept override;

  std::size_t currently_allocated() const;

 private:
  std::size_t _currently_allocated;
};

}  // namespace opossum
