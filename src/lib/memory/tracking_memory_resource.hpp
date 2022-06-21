#pragma once

#include <boost/container/pmr/memory_resource.hpp>
#include <boost/core/no_exceptions_support.hpp>
#include <atomic>

namespace opossum {

class TrackingMemoryResource : public boost::container::pmr::memory_resource {
 public:
  TrackingMemoryResource(); 
  void* do_allocate(std::size_t bytes, std::size_t alignment) override;
  void do_deallocate(void* p, std::size_t bytes, std::size_t alignment) override;
  [[nodiscard]] bool do_is_equal(const memory_resource& other) const BOOST_NOEXCEPT override;
  size_t get_amount() const;

 protected:
  // TODO: only let memory resource manager create new instances
  //friend class MemoryResourceManager;
  //TrackingMemoryResource(); 
  std::atomic_size_t _tracked_memory;
};

} // namespace opossum