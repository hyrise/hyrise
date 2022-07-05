#pragma once

#include <functional>
#include <map>

#include "memory/tracking_memory_resource.hpp"
#include "operators/abstract_operator.hpp"
#include "types.hpp"
#include "utils/meta_tables/abstract_meta_table.hpp"

namespace opossum {

class MemoryResourceManager : public Noncopyable {
 public:
  // TODO comment
  const std::map<std::pair<OperatorType, std::string>, std::shared_ptr<TrackingMemoryResource>>& memory_resources() const;
  std::shared_ptr<TrackingMemoryResource> get_memory_resource(const OperatorType operator_type, const std::string& operator_data_structure);

 protected:
  // make sure that only Hyrise can create new instances
  friend class Hyrise;
  MemoryResourceManager() = default;

  std::map<std::pair<OperatorType, std::string>, std::shared_ptr<TrackingMemoryResource>> _memory_resources;
};

}  // namespace opossum