#pragma once

#include <functional>
#include <unordered_map>

#include "types.hpp"
#include "utils/meta_tables/abstract_meta_table.hpp"

namespace opossum {

class TrackingMemoryResource;

class MemoryResourceManager : public Noncopyable {
 public:

  // TODO comment
  const std::unordered_map<std::string, std::shared_ptr<TrackingMemoryResource>>& memory_resources() const;
  const std::unordered_map<std::string, uint_64> get_current_memory_usage() const;
  std::shared_ptr<TrackingMemoryResource> get_memory_resource(const std::string& purpose) const;

 protected:
  // make sure that only Hyrise can create new instances
  friend class Hyrise;
  MemoryResourceManager(); // add "= default" ??

  std::unordered_map<std::string, std::shared_ptr<TrackingMemoryResource>> _memory_resources;
};

}  // namespace opossum