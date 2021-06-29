#include "dram_memory_resource.hpp"

#include <fstream>
#include <nlohmann/json.hpp>
#include <string>
#include <memkind.h>

namespace opossum {

DRAMMemoryResource::DRAMMemoryResource() {}

DRAMMemoryResource::~DRAMMemoryResource() {}

void* DRAMMemoryResource::do_allocate(std::size_t bytes, std::size_t alignment) {
  return memkind_malloc(MEMKIND_REGULAR, bytes);
}

void DRAMMemoryResource::do_deallocate(void* p, std::size_t bytes, std::size_t alignment) {
  memkind_free(MEMKIND_REGULAR, p);
}

bool DRAMMemoryResource::do_is_equal(const memory_resource& other) const noexcept { return true; }

memkind_t DRAMMemoryResource::kind() const { return MEMKIND_REGULAR; }

size_t DRAMMemoryResource::memory_usage() const {
  memkind_update_cached_stats();

  auto allocated = size_t{};

  const auto error = memkind_get_stat(kind(), MEMKIND_STAT_TYPE_ALLOCATED, &allocated);
  Assert(!error, "Error when retrieving MEMKIND_STAT_TYPE_ALLOCATED");
  return allocated;
}

}  // namespace opossum
