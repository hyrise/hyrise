#include "kind_memory_manager.hpp"

#include <boost/container/pmr/memory_resource.hpp>
// #pragma GCC diagnostic ignored "-Wdeprecated-dynamic-exception-spec"
#include <jemalloc/jemalloc.h>
#include <json.hpp>
#define MEMKIND_INTERNAL_API
#define JE_PREFIX
#include <memkind/internal/memkind_private.h>
#include <fstream>
#include <thread>

#include "dram_memory_resource.hpp"
#include "nvm_memory_resource.hpp"

#define MALLOCX_ARENA_MAX 0xffe
extern struct memkind *arena_registry_g[MALLOCX_ARENA_MAX];

namespace opossum {

KindMemoryManager::KindMemoryManager() {
  std::ifstream config_file("nvm.json");
  if (!config_file.good()) config_file = std::ifstream{"../nvm.json"};
  Assert(config_file.good(), "nvm.json not found");
  const nlohmann::json config = nlohmann::json::parse(config_file);

  (void)DRAMMemoryResource::get();

  for (auto it = config["kinds"].begin(); it != config["kinds"].end(); ++it) {
    if (it.value() == "nvm") {
      _resources.emplace(it.key(), NVMMemoryResource::get());
    } else if (it.value() == "dram") {
      _resources.emplace(it.key(), DRAMMemoryResource::get());
    } else {
      Fail(std::string("Unknown memory kind ") + it.value().get<std::string>());
    }
  }

  {
    bool stats_enabled;
    size_t stats_enabled_size{1};
    auto error_code = memkind_mallctl("config.stats", &stats_enabled, &stats_enabled_size, nullptr, 0);
    Assert(!error_code, "Cannot check if jemalloc was built with --stats_enabled");
    Assert(stats_enabled, "memkinds' jemalloc was not build with --stats_enabled");

    error_code = mallctl("config.stats", &stats_enabled, &stats_enabled_size, nullptr, 0);
    Assert(!error_code, "Cannot check if jemalloc was built with --stats_enabled");
    Assert(stats_enabled, "Hyrise's jemalloc was not build with --stats_enabled");
  }


  std::thread{[&](){
    std::ofstream file{"memory.log", std::ios::out};
    file << "DRAM,NVM,Unaccounted" << std::endl;

    const auto get_total_jemalloc = []() {
      size_t allocated;
      auto allocated_size = sizeof(allocated);

      char cmd[128];
      snprintf(cmd, 128, "stats.allocated");

      auto error_code = mallctl(cmd, &allocated, &allocated_size, nullptr, 0);
      if (error_code) {
        std::cout << "mallctl failed with error code " << std::to_string(error_code) << std::endl;
      }

      return allocated;
    };

    const auto get_total_memkind = [](const memkind_t& kind) {
      size_t total = 0;
      if (kind->arena_map_len) {
        // TODO lock removed here
        // TODO cache access to control structure

        for (auto i = 0u; i < kind->arena_map_len; ++i) {
          for (const auto& type : {"small", "large"}) {
            size_t allocated;
            auto allocated_size = sizeof(allocated);

            char cmd[128];
            snprintf(cmd, 128, "stats.arenas.%u.%s.allocated", kind->arena_zero + i, type);

            auto error_code = memkind_mallctl(cmd, &allocated, &allocated_size, nullptr, 0);
            if (error_code) {
              std::cout << "mallctl failed with error code " << std::to_string(error_code) << std::endl;
            } else {
              total += allocated;
            }
          }
        }
      }
      return total;
    };

    uint64_t epoch = 1;
    while (true) {
      // Update the statistics cached by mallctl.
      {
        epoch++;
        auto epoch_size = sizeof(epoch);
        mallctl("epoch", &epoch, &epoch_size, &epoch, epoch_size);
        memkind_mallctl("epoch", &epoch, &epoch_size, &epoch, epoch_size);
      }

      file << get_total_memkind(DRAMMemoryResource::get().kind()) << ",";
      file << get_total_memkind(NVMMemoryResource::get().kind()) << ",";
      file << get_total_jemalloc() << std::endl;

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

std::string KindMemoryManager::locate(const void* mem) {
    // size_t sz = sizeof(unsigned);
    // unsigned temp_arena;
    // int err = memkind_mallctl("arenas.lookup", &temp_arena, &sz, &mem, sizeof(mem));
    // if (err) return "unknown (error code " + std::to_string(err) + ")";
    // if (temp_arena >= DRAMMemoryResource::get().kind()->arena_zero && temp_arena < DRAMMemoryResource::get().kind()->arena_zero + DRAMMemoryResource::get().kind()->arena_map_len) return "DRAM";
    // if (temp_arena >= NVMMemoryResource::get().kind()->arena_zero && temp_arena < NVMMemoryResource::get().kind()->arena_zero + NVMMemoryResource::get().kind()->arena_map_len) return "NVM";
    return "unaccounted";
}

}  // namespace opossum
