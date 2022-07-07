#pragma once

#include <functional>
#include <map>

#include <oneapi/tbb/concurrent_vector.h>
#include "memory/tracking_memory_resource.hpp"
#include "types.hpp"
#include "utils/meta_tables/abstract_meta_table.hpp"

namespace opossum {

struct ResourceRecord {
  OperatorType operator_type;
  std::string operator_data_structure;
  std::shared_ptr<TrackingMemoryResource> resource_ptr;

  ResourceRecord(const OperatorType ot, const std::string& ods, std::shared_ptr<TrackingMemoryResource>& tmr)
      : operator_type{ot}, operator_data_structure{ods}, resource_ptr{tmr} {}
};

class MemoryResourceManager : public Noncopyable {
 public:
  const tbb::concurrent_vector<ResourceRecord>& memory_resources() const;
  std::shared_ptr<TrackingMemoryResource> get_memory_resource(const OperatorType operator_type,
                                                              const std::string& operator_data_structure);

 protected:
  // make sure that only Hyrise (and tests) can create new instances
  friend class Hyrise;
  friend class MemoryResourceManagerTest;
  MemoryResourceManager() = default;

  tbb::concurrent_vector<ResourceRecord> _memory_resources;
};

}  // namespace opossum