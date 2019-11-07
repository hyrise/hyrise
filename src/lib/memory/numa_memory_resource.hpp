#pragma once

#include <string>

#include <boost/container/pmr/memory_resource.hpp>
#include <boost/integer/common_factor_rt.hpp>

#if HYRISE_NUMA_SUPPORT
#include <PGASUS/msource/msource.hpp>
#endif

namespace opossum {

class NUMAMemoryResource : public boost::container::pmr::memory_resource {
 public:
  NUMAMemoryResource(int node_id, const std::string& name);

  virtual void* do_allocate(std::size_t bytes, std::size_t alignment);

  virtual void do_deallocate(void* p, std::size_t bytes, std::size_t alignment);

  virtual bool do_is_equal(const memory_resource& other) const noexcept;

  int get_node_id() const;

  static constexpr int UNDEFINED_NODE_ID = -1;

 private:
#if HYRISE_NUMA_SUPPORT
  const numa::MemSource _memory_source;
  const size_t _alignment = 1;
#endif
};

}  // namespace opossum
