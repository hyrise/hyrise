#pragma once

#if ENABLE_MEMKIND
#include <memkind.h>
#endif


#include <boost/container/pmr/memory_resource.hpp>
#include <utils/singleton.hpp>

namespace opossum {

class DRAMMemoryResource : public boost::container::pmr::memory_resource, public Singleton<DRAMMemoryResource> {
 public:
  virtual void* do_allocate(std::size_t bytes, std::size_t alignment);

  virtual void do_deallocate(void* p, std::size_t bytes, std::size_t alignment);

  virtual bool do_is_equal(const memory_resource& other) const noexcept;

  size_t memory_usage() const;

  memkind_t kind() const;

 protected:
  DRAMMemoryResource();
  ~DRAMMemoryResource();

  friend class Singleton;

  memkind_t _dram_kind;
};

}  // namespace opossum
