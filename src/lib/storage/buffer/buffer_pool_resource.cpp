#include "buffer_pool_resource.hpp"
#include "hyrise.hpp"

namespace hyrise {
BufferManagedPtr<void> BufferPoolResource::allocate(std::size_t bytes, std::size_t align) {
  Assert(bytes <= PAGE_SIZE, "Cannot allocate more than a Page currently");
  // TODO: Do Alignment with aligner, https://www.boost.org/doc/libs/1_62_0/doc/html/align.html
  const auto page_id = Hyrise::get().buffer_manager.new_page();
  return BufferManagedPtr<void>(page_id, 0);  // TODO: Use easier constrcutor without offset, no! alignment
}

void BufferPoolResource::deallocate(BufferManagedPtr<void> ptr, std::size_t bytes, std::size_t align) {
  Hyrise::get().buffer_manager.remove_page(ptr.get_page_id());
}

}  // namespace hyrise