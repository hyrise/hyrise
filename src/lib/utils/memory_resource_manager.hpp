#pragma once

#include <tbb/concurrent_vector.h>


#include "memory/tracking_memory_resource.hpp"
#include "types.hpp"
#include "utils/meta_tables/abstract_meta_table.hpp"

namespace opossum {

struct ResourceRecord {
  OperatorType operator_type;
  std::string operator_data_structure;
  TrackingMemoryResource* resource_ptr;

  ResourceRecord(const OperatorType ot, const std::string& ods, TrackingMemoryResource* tmr)
      : operator_type{ot}, operator_data_structure{ods}, resource_ptr{tmr} {}
};

class MemoryResourceManager : public Noncopyable {
 public:
  void enable();
  void disable();
  const tbb::concurrent_vector<ResourceRecord>& memory_resources() const;
  boost::container::pmr::memory_resource* get_memory_resource(const OperatorType operator_type,
                                                              const std::string& operator_data_structure);

 protected:
  // make sure that only Hyrise (and tests) can create new instances
  friend class Hyrise;
  friend class MemoryResourceManagerTest;
  MemoryResourceManager() = default;

  tbb::concurrent_vector<ResourceRecord> _memory_resources;
  bool _tracking_is_enabled = false;
};

}  // namespace opossum
