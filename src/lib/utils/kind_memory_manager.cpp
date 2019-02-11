#include "kind_memory_manager.hpp"

#include <json.hpp>
#include <fstream>

#include "nvm_memory_resource.hpp"

namespace opossum {

KindMemoryManager::KindMemoryManager() {
  std::ifstream config_file("nvm.json");
  if (!config_file.good()) config_file = std::ifstream{"../nvm.json"};
  Assert(config_file.good(), "nvm.json not found");
  const nlohmann::json config = nlohmann::json::parse(config_file);

  for (auto it = config["kinds"].begin(); it != config["kinds"].end(); ++it) {
    if (it.value() == "nvm") {
      _resources.emplace(it.key(), NVMMemoryResource::get());
    } else if (it.value() == "dram") {
      _resources.emplace(it.key(), *boost::container::pmr::get_default_resource());
    } else {
      Fail(std::string("Unknown memory kind ") + std::string(it.value()));
    }
  }
}

boost::container::pmr::memory_resource& KindMemoryManager::get_resource(const char* type) {
  auto it = _resources.find(type);
  if (it != _resources.end()) {
    return it->second;
  }
  return *boost::container::pmr::get_default_resource();
}

}  // namespace opossum
