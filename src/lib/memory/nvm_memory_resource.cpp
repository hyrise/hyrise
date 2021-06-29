#include "nvm_memory_resource.hpp"

#if ENABLE_MEMKIND

#include <memkind.h>

#include <fstream>
#include <nlohmann/json.hpp>
#include <string>

namespace opossum {

NVMMemoryResource::NVMMemoryResource() {
  std::ifstream config_file("nvm.json");
  if (!config_file.good()) config_file = std::ifstream{"../nvm.json"};
  Assert(config_file.good(), "nvm.json not found");
  const nlohmann::json config = nlohmann::json::parse(config_file);

  const auto pooldir = config["pooldir"].get<std::string>();
  Assert(std::filesystem::is_directory(pooldir), "NVM pooldir is not a directory");

  auto err = memkind_create_pmem(pooldir.c_str(), 0, &_nvm_kind);
  Assert(!err, "memkind_create_pmem failed");
}

NVMMemoryResource::~NVMMemoryResource() {
  //  Triggers kernel failure
  //  memkind_destroy_kind(_nvm_kind);
}

void* NVMMemoryResource::do_allocate(std::size_t bytes, std::size_t alignment) {
  auto p = memkind_malloc(_nvm_kind, bytes);

  return p;
}

void NVMMemoryResource::do_deallocate(void* p, std::size_t bytes, std::size_t alignment) {
  memkind_free(_nvm_kind, p);
}

bool NVMMemoryResource::do_is_equal(const memory_resource& other) const noexcept { return &other == this; }

memkind_t NVMMemoryResource::kind() const { return _nvm_kind; }

size_t NVMMemoryResource::memory_usage() const {
  memkind_update_cached_stats();

  auto allocated = size_t{};

  const auto error = memkind_get_stat(_nvm_kind, MEMKIND_STAT_TYPE_ALLOCATED, &allocated);
  Assert(!error, "Error when retrieving MEMKIND_STAT_TYPE_ALLOCATED");
  return allocated;
}

}  // namespace opossum

#endif