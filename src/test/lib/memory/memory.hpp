#pragma once

#include <algorithm>
#include <cstdint>
#include <limits>
#include <string>
#include <vector>

#include <numa.h>
#include <numaif.h>
#include <unistd.h>

namespace memory {

using NumaNodeID = uint32_t;
using NumaNodeIDs = std::vector<NumaNodeID>;
using InterleavingWeights = std::vector<uint32_t>;
using PageLocations = std::vector<int>;

class NumaNodeRingIterator {
  static constexpr uint64_t INVALID_VALUE = std::numeric_limits<uint64_t>::max();

 public:
  explicit NumaNodeRingIterator(const NumaNodeIDs& nodes) : _nodes(nodes), _next_position(INVALID_VALUE), _node_count(nodes.size()) {
    std::sort(_nodes.begin(), _nodes.end());
  };

  NumaNodeID next(uint64_t identified_node = INVALID_VALUE) {
    if (_next_position == INVALID_VALUE) {
      auto found = std::find(_nodes.begin(), _nodes.end(), identified_node);
      if (found == _nodes.end()) {
        throw std::logic_error("Value" + std::to_string(identified_node) + " not found in nodes.");
      }
      _next_position = std::distance(_nodes.begin(), found);
    }
    auto position = _next_position;
    _next_position = (_next_position + 1) % _node_count;
    return static_cast<NumaNodeID>(position);
  };

 private:
  NumaNodeIDs _nodes;
  uint64_t _next_position;
  uint64_t _node_count;
};

inline void numa_init() {
  numa_available();
  numa_set_strict(1);
}

inline NumaNodeIDs allowed_memory_node_ids() {
  const auto numa_max_node_id = numa_max_node();
  auto* const allowed_memory_nodes_mask = numa_get_mems_allowed();
  auto node_ids = NumaNodeIDs{};
  node_ids.reserve(numa_max_node_id);

  for (auto node_id = 0; node_id <= numa_max_node_id; ++node_id) {
    if (numa_bitmask_isbitset(allowed_memory_nodes_mask, node_id) == 0) {
      continue;
    }
    node_ids.push_back(static_cast<NumaNodeID>(node_id));
  }
  node_ids.shrink_to_fit();
  return node_ids;
}

inline NumaNodeID numa_node_of_address(void* addr) {
  int node_idx = -1;
  if (get_mempolicy(&node_idx, nullptr, 0, addr, MPOL_F_NODE | MPOL_F_ADDR) != 0) {
    auto ss = std::stringstream{};
    ss << "get_mempolicy failed: " << strerror(errno);
    throw std::logic_error(ss.str());
  }
  return static_cast<NumaNodeID>(node_idx);
}

template<typename T>
NumaNodeID numa_node_of_address(const T* addr) {
  auto void_addr = const_cast<void*>(reinterpret_cast<const void*>(addr));
  return numa_node_of_address(void_addr);
}

// Returns a vector containing the status (retrieved via move_pages) of each page in the passed buffer.
inline std::vector<int> get_page_statuses(std::byte* buffer, const uint64_t size) {
  const auto page_size = getpagesize();
  if (size % page_size != 0) {
    throw std::logic_error{"Buffer size is not a multiple of page size"};
  }
  const auto page_count = size / page_size;
  // move_pages requires a vector of void*, one void* per page.
  auto pages = std::vector<void*>{};
  pages.resize(page_count);
  for (auto page_idx = uint64_t{0}; page_idx < page_count; ++page_idx) {
    pages[page_idx] = reinterpret_cast<void*>(buffer + (page_idx * page_size));
  }

  auto page_status = std::vector<int>(page_count, std::numeric_limits<int>::max());
  // retrieve page status
  const auto ret = move_pages(0, page_count, pages.data(), nullptr, page_status.data(), MPOL_MF_MOVE);
  const auto move_pages_errno = errno;

  if (ret != 0) {
    throw std::logic_error{"move_pages() failed: " + std::string(strerror(move_pages_errno))};
  }
  return page_status;
}

} // namespace hyrise
