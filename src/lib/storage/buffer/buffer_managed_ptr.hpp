#pragma once

#include <cstddef>
#include "hyrise.hpp"
#include "types.hpp"
#include <boost/type_traits/add_reference.hpp>

namespace hyrise {

template <typename PointedType>
class BufferManagedPtr {
 public:
  // using Allocator = std::allocator<PointedType>;
  using pointer = PointedType*;
  using reference = typename boost::add_reference<PointedType>::type;
  using value_type = PointedType;
  using difference_type = int16_t;
  using iterator_category = std::random_access_iterator_tag;

  BufferManagedPtr() = default;

  // TODO: Offset = 0 should be invalid

  // BufferManagedPtr(pointer ptr = 0) : _page_id(INVALID_PAGE_ID), _offset(0) {  }

  explicit BufferManagedPtr(const PageID page_id, difference_type offset) : _page_id(page_id), _offset(offset) {}

  pointer operator->() const {
    return get();
  }

  reference operator*() const {
    // TODO: Check if pinned
    return get_pointer();
  }

  difference_type get_offset() const {
    return _offset;
  }

  PageID get_page_id() const {
    return _page_id;
  }

  reference operator[](std::ptrdiff_t idx) const {  
    return get()[idx];  
  }

  // BufferManagedPtr operator+ (std::ptrdiff_t offset) const  {  
  //   return BufferManagedPtr(_page_id, this->get() + offset);   
  // }

  // BufferManagedPtr operator- (std::ptrdiff_t offset) const  {  
  //   return BufferManagedPtr(_page_id, this->get() - offset);   
  // }

  pointer get() const {   
    return static_cast<pointer>(this->get_pointer());   
  }

  template <class U>
  explicit BufferManagedPtr(const BufferManagedPtr<U>& other) : _page_id(other.get_page_id()), _offset(other.get_offset()) {}

 private:
  static BufferManager& buffer_manager() {
    return Hyrise::get().buffer_manager;
  }

  void* get_pointer() const {
    auto page = buffer_manager().get_page(_page_id);
    return page->data.data() + _offset;
  }

  PageID _page_id;
  difference_type _offset;
};

template <class PointedType, class... Args>
BufferManagedPtr<PointedType> allocate_buffer_managed(Args&&... args) {
  return BufferManagedPtr<PointedType>();
}
}  // namespace hyrise