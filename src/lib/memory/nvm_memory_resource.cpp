#include "nvm_memory_resource.hpp"

#include <fstream>
#include <json.hpp>
#include <string>

namespace opossum {

NVMMemoryResource::NVMMemoryResource() {
  std::ifstream config_file("nvm.json");
  if (!config_file.good()) config_file = std::ifstream{"../nvm.json"};
  Assert(config_file.good(), "nvm.json not found");
  const nlohmann::json config = nlohmann::json::parse(config_file);

  const auto pooldir = config["pooldir"].get<std::string>();

  auto err = memkind_create_pmem(pooldir.c_str(), 0, &_nvm_kind);
  Assert(!err, "memkind_create_pmem failed");
}

NVMMemoryResource::~NVMMemoryResource() {
  //  memkind_destroy_kind(_nvm_kind);
}

void* NVMMemoryResource::do_allocate(std::size_t bytes, std::size_t alignment) {
  return memkind_malloc(_nvm_kind, bytes);
}

void NVMMemoryResource::do_deallocate(void* p, std::size_t bytes, std::size_t alignment) { memkind_free(_nvm_kind, p); }

bool NVMMemoryResource::do_is_equal(const memory_resource& other) const noexcept { return true; }

memkind_t NVMMemoryResource::kind() const { return _nvm_kind; }

}  // namespace opossum
