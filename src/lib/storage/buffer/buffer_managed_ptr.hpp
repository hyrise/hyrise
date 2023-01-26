#pragma once

#include <cstddef>
#include "storage/buffer/types.hpp"

namespace hyrise {

template <typename PointedType>
class BufferManagedPtr {
 public:
  using pointer = PointedType*;
  using reference = typename add_reference<PointedType>::type;
  using element_type = PointedType;
  using difference_type = std::ptrdiff_t;  // TODO: Remove page offfset
  // using iterator_category = std::random_access_iterator_tag;
  using iterator_category = std::contiguous_iterator_tag;

  // Check offset pointer for alignment
  // Offset type it not difference type
  // TODO: Offset = 0 should be invalid, check other type for that when transforming into pinter and back
  // https://github.com/darwin/boost/blob/master/boost/interprocess/offset_ptr.hpp
  // https://www.youtube.com/watch?v=_nIET46ul6E
  // Segment ptr uses pointer swizzlhttps://github.com/boostorg/interprocess/blob/4403b201bef142f07cdc43f67bf6477da5e07fe3/include/boost/interprocess/detail/intersegment_ptr.hpp#L611
  // A lot of things are copied form offset_ptr
  BufferManagedPtr(pointer ptr = 0);

  // BufferManagedPtr(std::nullptr_t);

  BufferManagedPtr(const BufferManagedPtr& ptr) : _page_id(ptr.get_page_id()), _offset(ptr.get_offset()) {}

  template <class U>
  explicit BufferManagedPtr(const BufferManagedPtr<U>& other)
      : _page_id(other.get_page_id()), _offset(other.get_offset()) {}

  // TODO
  template <class T>
  BufferManagedPtr(T* ptr);

  explicit BufferManagedPtr(const PageID page_id, difference_type offset) : _page_id(page_id), _offset(offset) {}

  pointer operator->() const {
    return get();
  }

  reference operator*() const {
    // TODO: Check if pinned, and try to reduce branching
    pointer p = this->get();
    reference r = *p;
    return r;
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

  bool operator!() const {
    return this->get() == 0;
  }

  BufferManagedPtr operator+(std::ptrdiff_t offset) const {
    return BufferManagedPtr(_page_id, this->_offset + offset);
  }

  BufferManagedPtr operator-(std::ptrdiff_t offset) const {
    return BufferManagedPtr(_page_id, this->_offset - offset);
  }

  BufferManagedPtr& operator=(const BufferManagedPtr& ptr) {
    // pointer p(pt.get());  (void)p; this->set_offset(p);
    _page_id = ptr.get_page_id();
    _offset = ptr.get_offset();
    return *this;
  }

  explicit operator bool() const noexcept {
    return this->_page_id == INVALID_PAGE_ID || this->_offset == 0;
  }

  pointer get() const {
    return static_cast<pointer>(this->get_pointer());
  }

  static pointer to_address(BufferManagedPtr ptr) {
    return ptr.get();
  }

  static BufferManagedPtr pointer_to(reference r) {
    return BufferManagedPtr(&r);
  }

  friend bool operator==(const BufferManagedPtr& ptr1, const BufferManagedPtr& ptr2) noexcept {
    return ptr1.get() == ptr2.get();
  }

  friend bool operator==(pointer ptr1, const BufferManagedPtr& ptr2) noexcept {
    return ptr1 == ptr2.get();
  }

  void* get_pointer() const;

 private:
  PageID _page_id;  // TODO: Make const
  difference_type _offset;
};

template <class T1, class T2>
inline bool operator==(const BufferManagedPtr<T1>& ptr1, const BufferManagedPtr<T2>& ptr2) {
  return ptr1.get() == ptr2.get();
}

template <class T1, class T2>
inline bool operator!=(const BufferManagedPtr<T1>& ptr1, const BufferManagedPtr<T2>& ptr2) {
  return ptr1.get() != ptr2.get();
}

template <class T>
inline BufferManagedPtr<T> operator+(std::ptrdiff_t diff, const BufferManagedPtr<T>& right) {
  return right + diff;
}

template <class T, class T2>
inline std::ptrdiff_t operator-(const BufferManagedPtr<T>& pt, const BufferManagedPtr<T2>& pt2) {
  return pt.get() - pt2.get();
}

template <class PointedType, class... Args>
BufferManagedPtr<PointedType> allocate_buffer_managed(Args&&... args) {
  return BufferManagedPtr<PointedType>();
}



// TODO: Remove later when integrated
extern template class BufferManagedPtr<int>;

}  // namespace hyrise

namespace std {
template <class T>
struct hash<hyrise::BufferManagedPtr<T>> {
  size_t operator(const hyrise::BufferManagedPtr<T> & ptr) const noexcept
      const auto page_id_hash = std::hash<PageID>{}(ptr.get_page_id());
      const auto offset_hash = std::hash<hyrise::BufferManagedPtr<T>::difference_type>{}(ptr.get_offset());
      return boost::hash_combine(page_id_hash, offset_hash);
  }
};
}