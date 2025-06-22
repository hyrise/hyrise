#pragma once

#include <cstddef>
#include <memory>
#include <memory_resource>
#include <mutex>
#include <vector>
#include <sstream>

namespace hyrise {

using NumaNodeID = uint32_t;
using NumaNodeIDs = std::vector<NumaNodeID>;
using InterleavingWeights = std::vector<uint32_t>;
using PageLocations = std::vector<int>;
constexpr uint64_t SIZE_GIB = 1024u * 1024 * 1024;

class LinearNumaMemoryResource : public std::pmr::memory_resource {
 private:
  std::byte* _start_addr;
  std::byte* _alloc_addr;
  uint64_t _buffer_size;
  NumaNodeIDs _node_ids;
  InterleavingWeights _node_weights;
  uint64_t _alloc_offset;
  uint32_t _page_size;
  std::unique_ptr<std::mutex> _mutex;

 public:
  LinearNumaMemoryResource(const uint64_t buffer_size, const NumaNodeIDs node_ids,
    const InterleavingWeights node_weights = {});
  LinearNumaMemoryResource(LinearNumaMemoryResource&& other) noexcept;
  LinearNumaMemoryResource(const LinearNumaMemoryResource&) = delete;
  LinearNumaMemoryResource& operator=(const LinearNumaMemoryResource&) = delete;
  LinearNumaMemoryResource& operator=(LinearNumaMemoryResource&&) = delete;
  ~LinearNumaMemoryResource() override;

  bool address_in_range(const void* addr) const;

  std::byte* data() const;

  const NumaNodeIDs& node_ids() const;

  const InterleavingWeights& node_weights() const;

  static void fill_page_locations_weighted_interleaved(PageLocations& page_locations, uint64_t buffer_size,
      const NumaNodeIDs&node_ids, const InterleavingWeights& node_weights);

  static void place_pages(std::byte* const start_addr, uint64_t buffer_size, const PageLocations& target_page_locations);

  bool is_weighted_interleaved() const;

  std::string description() const;

 private:
  void* do_allocate(std::size_t bytes, std::size_t alignment) override;

  void do_deallocate(void* ptr, std::size_t bytes, std::size_t alignment) override;

  bool do_is_equal(const std::pmr::memory_resource& other) const noexcept override;

  void _bind_memory_round_robin_interleaved();

};

}  // namespace hyrise
