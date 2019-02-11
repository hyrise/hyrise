#pragma once

#include <boost/container/pmr/memory_resource.hpp>

#include "singleton.hpp"

namespace opossum {

class KindMemoryManager : public Singleton<KindMemoryManager> {
 public:
  boost::container::pmr::memory_resource& get_resource(const char* type);

 protected:
  KindMemoryManager();
  std::unordered_map<std::string, std::reference_wrapper<boost::container::pmr::memory_resource>> _resources;

  friend class Singleton;
};

}  // namespace opossum
