#include "nvm_memory_resource.hpp"

#include <libpmemobj++/allocator.hpp>
#include <libpmemobj++/transaction.hpp>
#include <string>

namespace opossum {

NVMMemoryResource::NVMMemoryResource() {
  _nvm_pool = decltype(_nvm_pool)::create("poolfile", "my_identifier", 1'000'000'000ul);
}

void* NVMMemoryResource::do_allocate(std::size_t bytes, std::size_t alignment) {
  void* raw_pointer;
  pmem::obj::transaction::run(_nvm_pool, [&] {
    pmem::obj::allocator<char> allocator;
    auto persistent_pointer = allocator.allocate(bytes);
    raw_pointer = reinterpret_cast<void*>(&*persistent_pointer);
    // std::cout << "alloc " << bytes << ", align " << alignment << ", persistent_pointer: " << persistent_pointer << ", raw_pointer: " << raw_pointer <<  ", total " << (_total_alloc += bytes) << std::endl;
    // std::cout << "inside state " << pmemobj_tx_stage() << std::endl;
  });
  // std::cout << "outside state " << pmemobj_tx_stage() << std::endl;
  return raw_pointer;
}

void NVMMemoryResource::do_deallocate(void* p, std::size_t bytes, std::size_t alignment) {
  std::cout << "not deallocating " << bytes << " bytes" << std::endl;
  // pmem::obj::transaction::run(_nvm_pool, [&] {
  //   pmem::obj::allocator<char> allocator;
  //   allocator.deallocate(p);
  // });
}

bool NVMMemoryResource::do_is_equal(const memory_resource& other) const noexcept { return true; }

}  // namespace opossum
