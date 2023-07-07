#pragma once

#include <cstdint>

namespace hyrise {


class NumaMemoryResource : public boost::container::pmr::memory_resource {
 public:
  NumaMemoryResource(const NodeID node_id);

  void* do_allocate(std::size_t bytes, std::size_t alignment) override;
  void do_deallocate(void* p, std::size_t bytes, std::size_t alignment) override;
  bool do_is_equal(const memory_resource& other) const noexcept override;
  size_t lap_num_allocations(); 

  size_t _num_allocations; 
  size_t _num_deallocations;
  size_t _sum_allocated_bytes;  
 protected:
  NodeID _node_id;
  size_t _lap_num_allocations; 
};

}  // namespace hyrise