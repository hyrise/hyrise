#pragma once

#include <boost/container/pmr/memory_resource.hpp>
#include <libpmemobj++/pool.hpp>
#include <utils/singleton.hpp>

namespace opossum {

class NVMMemoryResource : public boost::container::pmr::memory_resource, public Singleton<NVMMemoryResource> {
 public:
  virtual void* do_allocate(std::size_t bytes, std::size_t alignment);

  virtual void do_deallocate(void* p, std::size_t bytes, std::size_t alignment);

  virtual bool do_is_equal(const memory_resource& other) const noexcept;

 protected:
  friend class Singleton;
  NVMMemoryResource();
  struct nvm_root {};
  pmem::obj::pool<nvm_root> _nvm_pool;

  size_t _total_alloc;
};

}  // namespace opossum
