#pragma once

#include <boost/container/pmr/memory_resource.hpp>
#include <boost/math/common_factor_rt.hpp>
#include <string>

#if OPOSSUM_NUMA_SUPPORT
#include <PGASUS/msource/msource.hpp>
#endif

namespace opossum {

class NUMAMemoryResource : public boost::container::pmr::memory_resource {
 public:
  NUMAMemoryResource(int node_id, const std::string &name);

  virtual void* do_allocate(std::size_t bytes, std::size_t alignment);

  virtual void do_deallocate(void* p, std::size_t bytes, std::size_t alignment);

  virtual bool do_is_equal(const memory_resource& other) const noexcept;

  static NUMAMemoryResource* get_default_resource();

 private:
#if OPOSSUM_NUMA_SUPPORT
  const numa::MemSource _mem_source;
#endif
  const size_t _alignment = 1;
};

}  // namespace opossum
