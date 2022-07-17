#pragma once

#include <atomic>
#include <chrono>

#include <boost/container/pmr/memory_resource.hpp>
#include <boost/core/no_exceptions_support.hpp>

namespace opossum {

class TrackingMemoryResource : public boost::container::pmr::memory_resource {
 public:
  TrackingMemoryResource() = default;
  void* do_allocate(std::size_t bytes, std::size_t alignment) override;
  void do_deallocate(void* p, std::size_t bytes, std::size_t alignment) override;
  [[nodiscard]] bool do_is_equal(const memory_resource& other) const BOOST_NOEXCEPT override;
  const std::vector<std::pair<std::chrono::system_clock::time_point, int64_t>>& memory_timeseries() const;

 protected:
  std::vector<std::pair<std::chrono::system_clock::time_point, int64_t>> _memory_timeseries;
  std::chrono::system_clock::time_point _get_timestamp() const;
};

}  // namespace opossum