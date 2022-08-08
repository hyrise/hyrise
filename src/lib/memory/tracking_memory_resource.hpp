#pragma once

#include <chrono>
#include <utility>
#include <vector>

#include <boost/container/pmr/memory_resource.hpp>
#include <boost/core/no_exceptions_support.hpp>

namespace opossum {

/*
 * This class stores all (de-) allocations with timestamps as a pair in a vector by overwriting the default memory_resource.
 * It can be used in polymorphic allocators to track the memory usage of containers. The allocation behaviour itself remains unchanged.
 */
class TrackingMemoryResource : public boost::container::pmr::memory_resource {
 public:
  TrackingMemoryResource() = default;
  void* do_allocate(std::size_t bytes, std::size_t alignment) override;
  void do_deallocate(void* pointer, std::size_t bytes, std::size_t alignment) override;
  [[nodiscard]] bool do_is_equal(const memory_resource& other) const BOOST_NOEXCEPT override;
  const std::vector<std::pair<std::chrono::system_clock::time_point, int64_t>>& memory_timeseries() const;

 protected:
  std::vector<std::pair<std::chrono::system_clock::time_point, int64_t>> _memory_timeseries;
};

}  // namespace opossum
