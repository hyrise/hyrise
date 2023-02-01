#include "buffer_pool_resource.hpp"
#include "hyrise.hpp"

namespace hyrise {
BufferManagedPtr<void> BufferPoolResource::allocate(std::size_t bytes, std::size_t align) {
  // TODO: Assert(bytes <= PAGE_SIZE, "Cannot allocate more than a Page currently");
  const auto page_id = Hyrise::get().buffer_manager.new_page();
  return BufferManagedPtr<void>(page_id, PageOffset{1}); // TODO: Use easier constrcutor without offset
}

void BufferPoolResource::deallocate(BufferManagedPtr<void> ptr, std::size_t bytes, std::size_t align) {
  Hyrise::get().buffer_manager.remove_page(ptr.get_page_id());
}

}  // namespace hyrise