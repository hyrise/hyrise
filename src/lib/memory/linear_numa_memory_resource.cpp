#include "linear_numa_memory_resource.hpp"

#include <numa.h>
#include <numaif.h>
#include <sys/mman.h>
#include <unistd.h>

#include <cerrno>
#include <iostream>
#include <memory>
#include <mutex>
#include <utility>
#include <vector>

namespace hyrise {

LinearNumaMemoryResource::LinearNumaMemoryResource(const uint64_t buffer_size, const NumaNodeIDs node_ids,
  const InterleavingWeights node_weights)
    : _buffer_size(buffer_size), _node_ids(node_ids), _node_weights(node_weights), _alloc_offset(0),
      _page_size(getpagesize()), _mutex(std::make_unique<std::mutex>()) {
  // Init NUMA
  static auto numa_initialized = false;
  if (!numa_initialized) {
    numa_available();
    numa_set_strict(1);
    numa_initialized = true;
  }
  // MAP_NORESERVE?
  _start_addr =
      static_cast<std::byte*>(mmap(nullptr, _buffer_size, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0));

  if (node_weights.empty()) {
    _bind_memory_round_robin_interleaved();
    std::cout << description() << ": Bound region for round robin page interleaving.\n" << std::flush;
  }

  // Prefaulting
  const auto page_count = _buffer_size / _page_size;
  for (auto page_id = uint64_t{0}; page_id < page_count; ++page_id) {
    _start_addr[page_id * _page_size] = std::byte{0};
  }

  if (!_node_weights.empty()) {
    auto page_locations = PageLocations{};
    fill_page_locations_weighted_interleaved(page_locations, _buffer_size, _node_ids, _node_weights);
    place_pages(_start_addr, _buffer_size, page_locations);
    std::cout << description() << ": Moved pages for weighted interleaving.\n" << std::flush;
  }

  _alloc_addr = _start_addr;
}

LinearNumaMemoryResource::LinearNumaMemoryResource(LinearNumaMemoryResource&& other) noexcept :
  _start_addr(other._start_addr), _alloc_addr(other._alloc_addr), _buffer_size(other._buffer_size),
  _node_ids(std::move(other._node_ids)), _node_weights(std::move(other._node_weights)),
  _alloc_offset(other._alloc_offset), _page_size(other._page_size), _mutex(std::move(other._mutex)) {
  // Do nothing.
}

LinearNumaMemoryResource::~LinearNumaMemoryResource() {
  munmap(_start_addr, _buffer_size);
}

std::byte* LinearNumaMemoryResource::data() const {
  return _start_addr;
}

const NumaNodeIDs& LinearNumaMemoryResource::node_ids() const {
  return _node_ids;
}

std::string LinearNumaMemoryResource::description() const {
  auto sstream = std::stringstream{};
  for (auto& value : node_ids()) {
    if (sstream.str().empty()) {
      sstream << value;
    } else {
      sstream << ", " << value;
    }
  }
  return std::string{"LinearNuma "} + sstream.str();
}

const InterleavingWeights& LinearNumaMemoryResource::node_weights() const {
  return _node_weights;
}

void* LinearNumaMemoryResource::do_allocate(std::size_t bytes, std::size_t alignment) {
  auto lock = std::lock_guard{*_mutex};
  auto alloc_addr = reinterpret_cast<std::uintptr_t>(_alloc_addr);
  auto aligned_addr = reinterpret_cast<std::byte*>((alloc_addr + (alignment - 1)) & ~(alignment - 1));

  if ((aligned_addr + bytes) > (_start_addr + _buffer_size)) {
    throw std::logic_error("Not enough buffer capacity");
  }
  _alloc_addr = aligned_addr + bytes;
  return aligned_addr;
}

void LinearNumaMemoryResource::do_deallocate(void* ptr, std::size_t bytes, std::size_t alignment) {
  // Nothing.
}

bool LinearNumaMemoryResource::do_is_equal(const std::pmr::memory_resource& other) const noexcept {
  return (this == &other);
}

void LinearNumaMemoryResource::_bind_memory_round_robin_interleaved() {
  if (_node_ids.empty()) {
    return;
  }
  const auto* const allowed_memory_nodes_mask = numa_get_mems_allowed();
  auto* const nodemask = numa_allocate_nodemask();
  for (const auto node_id : _node_ids) {
    if (!numa_bitmask_isbitset(allowed_memory_nodes_mask, node_id)) {
      throw std::logic_error("Memory allocation on numa node id not allowed (given: " + std::to_string(node_id) + ").");
    }
    numa_bitmask_setbit(nodemask, node_id);
  }

  // Note that "[i]f the MPOL_INTERLEAVE policy was specified, pages already residing on the specified nodes will not be
  // moved such that they are interleaved", see mbind(2) manual.
  if (!nodemask) {
    throw std::logic_error{"When setting the memory nodes, node mask cannot be nullptr."};
  }
  const auto mbind_succeeded = mbind(_start_addr, _buffer_size, MPOL_INTERLEAVE, nodemask->maskp, nodemask->size + 1,
                                     MPOL_MF_STRICT | MPOL_MF_MOVE) == 0;
  const auto mbind_errno = errno;
  if (!mbind_succeeded) {
    auto ss = std::stringstream{};
    ss << "mbind failed: " << strerror(mbind_errno)
       << ". You might have run out of space on the node(s) or reached the maximum map "
       << "limit (vm.max_map_count).";
    throw std::logic_error(ss.str());
  }
  numa_bitmask_free(nodemask);
}

void LinearNumaMemoryResource::fill_page_locations_weighted_interleaved(PageLocations& page_locations,
  uint64_t buffer_size, const NumaNodeIDs& node_ids, const InterleavingWeights& node_weights) {
  const auto page_size = getpagesize();
  const auto page_count = buffer_size / page_size;
  auto cur_node_position = 0u;
  auto cur_node_page_count = 0u;
  const auto node_count = node_ids.size();

  page_locations.resize(page_count);
  for (auto page_idx = 0u; page_idx < page_count; ++page_idx) {
    if (cur_node_page_count == node_weights[cur_node_position]) {
      cur_node_position = (cur_node_position + 1) % node_count;
      cur_node_page_count = 0;
    }
    page_locations[page_idx] = node_ids[cur_node_position];
    ++cur_node_page_count;
  }
}

void LinearNumaMemoryResource::place_pages(std::byte* const start_addr, uint64_t buffer_size,
  const PageLocations& target_page_locations) {
  if (target_page_locations.empty()) {
    throw std::logic_error("Page placement failed: no target page locations are given.");
  }

  const auto page_size = getpagesize();
  const auto page_count = buffer_size / page_size;

  // move_pages requires a vector of void*, one void* per page.
  auto pages = std::vector<void*>{};
  pages.resize(page_count);

  for (auto page_idx = uint64_t{0}; page_idx < page_count; ++page_idx) {
    pages[page_idx] = reinterpret_cast<void*>(start_addr + (page_idx * page_size));
  }

  // move_pages, initialize with value that is not a valid NUMA node idx. We assume that max int is not a valid idx.
  auto page_status = std::vector<int>(page_count, std::numeric_limits<int>::max());

  const auto ret =
      move_pages(0, page_count, pages.data(), target_page_locations.data(), page_status.data(), MPOL_MF_MOVE);
  const auto move_pages_errno = errno;

  if (ret != 0) {
    throw std::logic_error(std::string{"move_pages failed: "} + strerror(move_pages_errno));
  }
}

bool LinearNumaMemoryResource::address_in_range(const void* addr) const {
  return _start_addr <= addr && addr < _start_addr + _buffer_size;
}

bool LinearNumaMemoryResource::is_weighted_interleaved() const {
  return !_node_weights.empty() && _node_weights.size() == _node_ids.size();
}

}  // namespace hyrise
