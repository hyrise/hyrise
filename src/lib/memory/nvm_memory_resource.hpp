#pragma once

#if ENABLE_MEMKIND
#include <memkind.h>
#endif

#include <boost/container/pmr/memory_resource.hpp>
#include <utils/singleton.hpp>

namespace opossum {

#if ENABLE_MEMKIND

class NVMMemoryResource : public boost::container::pmr::memory_resource, public Singleton<NVMMemoryResource> {
 public:
  virtual void* do_allocate(std::size_t bytes, std::size_t alignment);

  virtual void do_deallocate(void* p, std::size_t bytes, std::size_t alignment);

  virtual bool do_is_equal(const memory_resource& other) const noexcept;

  memkind_t kind() const;

  size_t memory_usage() const;

 protected:
  NVMMemoryResource();
  ~NVMMemoryResource();

  friend class Singleton;

  memkind_t _dram_kind;
  memkind_t _nvm_kind;
};

#else

class NVMMemoryResource : public boost::container::pmr::memory_resource, public Singleton<NVMMemoryResource> {
 public:
  void* do_allocate(std::size_t bytes, std::size_t alignment) override { return std::malloc(bytes); }  // NOLINT

  void do_deallocate(void* p, std::size_t bytes, std::size_t alignment) override { std::free(p); }  // NOLINT

  [[nodiscard]] bool do_is_equal(const memory_resource& other) const BOOST_NOEXCEPT override { return &other == this; }

  size_t memory_usage() const { return 0; }
};

#endif

}  // namespace opossum
