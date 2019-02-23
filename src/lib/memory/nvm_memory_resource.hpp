#pragma once

#include <memkind.h>
#include <boost/container/pmr/memory_resource.hpp>
#include <utils/singleton.hpp>

namespace opossum {

class NVMMemoryResource : public boost::container::pmr::memory_resource, public Singleton<NVMMemoryResource> {
 public:
  virtual void* do_allocate(std::size_t bytes, std::size_t alignment);

  virtual void do_deallocate(void* p, std::size_t bytes, std::size_t alignment);

  virtual bool do_is_equal(const memory_resource& other) const noexcept;

  memkind_t kind() const;

 protected:
  NVMMemoryResource();
  ~NVMMemoryResource();

  friend class Singleton;

  memkind_t _nvm_kind;
};

}  // namespace opossum
