#include "buffer_managed_ptr.hpp"
#include "hyrise.hpp"
#include "storage/buffer/buffer_manager.hpp"
#include "types.hpp"

namespace hyrise {

template <typename PointedType>  // TODO: Fix empty init
BufferManagedPtr<PointedType>::BufferManagedPtr(pointer ptr) : _page_id(INVALID_PAGE_ID), _offset(0) {}

template <typename PointedType>
template <class T>
BufferManagedPtr<PointedType>::BufferManagedPtr(T* ptr) {
  const auto [page_id, offset] = Hyrise::get().buffer_manager.get_page_id_and_offset_from_ptr(ptr);
  _page_id = page_id;
  _offset = offset;  // TODO: Maybe this should be +1
}

template <typename PointedType>
void* BufferManagedPtr<PointedType>::get_pointer() const {
  const auto page = Hyrise::get().buffer_manager.get_page(_page_id);
  return page->data.data() + _offset;
}

// TODO: Remove later when integrated
template class BufferManagedPtr<int>;

}  // namespace hyrise
