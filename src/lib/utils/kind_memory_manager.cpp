#include "kind_memory_manager.hpp"

#include <json.hpp>
#define MEMKIND_INTERNAL_API
#define JE_PREFIX
#include <memkind/internal/memkind_private.h>
#include <fstream>
#include <thread>

#include "nvm_memory_resource.hpp"

#define MALLOCX_ARENA_MAX 0xffe
extern struct memkind *arena_registry_g[MALLOCX_ARENA_MAX];

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

  {
    bool stats_enabled;
    size_t stats_enabled_size{1};
    auto error_code = memkind_mallctl("config.stats", &stats_enabled, &stats_enabled_size, nullptr, 0);
    Assert(!error_code, "Cannot check if jemalloc was built with --stats_enabled");
    Assert(stats_enabled, "jemalloc was not build with --stats_enabled");
  }


  std::thread{[&](){
    uint64_t epoch = 1;
    while (true) {
      // Update the statistics cached by mallctl.
      {
        epoch++;
        auto epoch_size = sizeof(epoch);
        memkind_mallctl("epoch", &epoch, &epoch_size, &epoch, epoch_size);
      }

      size_t nvm_size = 0;

      const auto nvm_kind = NVMMemoryResource::get().kind();
      if (nvm_kind->arena_map_len) {
        // TODO lock removed here
        // TODO cache access to control structure


        for (auto i = 0u; i < nvm_kind->arena_map_len; ++i) {
          size_t mapped;
          auto mapped_size = sizeof(mapped);

          char cmd[128];
          snprintf(cmd, 128, "stats.arenas.%u.mapped", nvm_kind->arena_zero + i);

          auto error_code = memkind_mallctl(cmd, &mapped, &mapped_size, nullptr, 0);
          if (error_code) {
            std::cout << "mallctl failed with error code " << std::to_string(error_code) << std::endl;
          } else {
            nvm_size += mapped;
          }
        }
      }

      std::cout << "NVM: " << nvm_size << std::endl;

      sleep(1);
    }
  }}.detach();
}

boost::container::pmr::memory_resource& KindMemoryManager::get_resource(const char* type) {
  auto it = _resources.find(type);
  if (it != _resources.end()) {
    return it->second;
  }
  Fail(std::string("Memory kind not defined for ") + type);
}

}  // namespace opossum
