#include "dram_memory_resource.hpp"

#include <fstream>
#include <json.hpp>
#include <string>
#define MEMKIND_INTERNAL_API
#define JE_PREFIX
#include <memkind/internal/memkind_private.h>

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

}  // namespace opossum
