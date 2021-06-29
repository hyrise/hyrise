#pragma once

#include <boost/container/pmr/memory_resource.hpp>
#include <utils/singleton.hpp>

#if HYRISE_WITH_UMAP && HYRISE_WITH_JEMALLOC
#include <jemalloc/jemalloc.h>
#endif

// #define UMAP_JEMALLOC_APPROACH

namespace opossum {

#if HYRISE_WITH_UMAP && HYRISE_WITH_JEMALLOC

// TODO migration is not thread-safe
#ifdef UMAP_JEMALLOC_APPROACH
class UmapMemoryResource : public boost::container::pmr::memory_resource, public Singleton<UmapMemoryResource> {
 public:
  UmapMemoryResource();
#else
class UmapMemoryResource : public boost::container::pmr::memory_resource {
 public:
  UmapMemoryResource(const std::string& filename);
#endif

  virtual void* do_allocate(std::size_t bytes, std::size_t alignment);

  virtual void do_deallocate(void* p, std::size_t bytes, std::size_t alignment);

  virtual bool do_is_equal(const memory_resource& other) const noexcept;

  size_t memory_usage() const;

  static constexpr size_t ALLOCATED_SIZE = 4096lu * 4096lu * 4096lu;  //68 GB

 protected:
  ~UmapMemoryResource();

  const std::string _filename;
  int fd;
  void *_base_addr;
  size_t _offset{};

  extent_hooks_t _hooks{};
  unsigned int _mallocx_flags;
  int _arena_index;
};

#else

static_assert(false, "UMAP missing");

class UmapMemoryResource : public boost::container::pmr::memory_resource, public Singleton<UmapMemoryResource> {
 public:
  UmapMemoryResource();
  // UmapMemoryResource(const std::string& filename);

  void* do_allocate(std::size_t bytes, std::size_t alignment) override {Fail("Either HYRISE_WITH_UMAP or HYRISE_WITH_JEMALLOC not set"); }  // NOLINT

  void do_deallocate(void* p, std::size_t bytes, std::size_t alignment) override { }  // NOLINT

  [[nodiscard]] bool do_is_equal(const memory_resource& other) const BOOST_NOEXCEPT override { return false; }

  size_t memory_usage() const { return 0; }
};

#endif

}  // namespace opossum
