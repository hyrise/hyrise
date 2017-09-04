#pragma once

#include <boost/container/pmr/memory_resource.hpp>

#include <boost/math/common_factor_rt.hpp>
#include <PGASUS/msource/msource.hpp>

namespace opossum {

class NUMAMemoryResource : public boost::container::pmr::memory_resource {
 public:
  NUMAMemoryResource(int node_id, size_t arena_size, const char* name, size_t mmap_threshold);

  virtual void* do_allocate(std::size_t bytes, std::size_t alignment) override;

  virtual void do_deallocate(void* p, std::size_t bytes, std::size_t alignment) override;

  virtual bool do_is_equal(const memory_resource& other) const noexcept override;

  static NUMAMemoryResource* get_default_resource();

 private:
  const numa::MemSource _mem_source;
  const size_t _alignment = 1;
};

}  // namespace opossum