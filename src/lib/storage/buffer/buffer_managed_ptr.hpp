#pragma once

#include <cstddef>
#include "storage/buffer/buffer_manager.hpp"
#include "storage/buffer/types.hpp"
#include "utils/assert.hpp"

namespace hyrise {

template <typename PointedType>
class BufferManagedPtr {
 public:
  using pointer = PointedType*;
  using reference = typename add_reference<PointedType>::type;
  using element_type = PointedType;
  using value_type = std::remove_cv_t<PointedType>;  // TODO: is std::remove_cv_t a good idea?
  using difference_type = std::ptrdiff_t;            // TODO: Remove page offfset
  using iterator_category = std::random_access_iterator_tag;

  // TODO: This does not compile when unordered map/set iteratorsare used
  // using iterator_category = std::contiguous_iterator_tag;
  // it seems like boost iterator_enable_if_tag does not it up
  // using iterator_concept = std::random_access_iterator_tag;  // TODO: contiguous_iterator_tag
  // TODO: introduce custom iterator type, that keeps the pointer/page in mem all the time, but works with raw pointer
  // Check offset pointer for alignment
  // Offset type it not difference type
  // TODO: Offset = 0 should be invalid, check other type for that when transforming into pinter and back
  // https://github.com/darwin/boost/blob/master/boost/interprocess/offset_ptr.hpp
  // https://www.youtube.com/watch?v=_nIET46ul6E
  // Segment ptr uses pointer swizzlhttps://github.com/boostorg/interprocess/blob/4403b201bef142f07cdc43f67bf6477da5e07fe3/include/boost/interprocess/detail/intersegment_ptr.hpp#L611
  // A lot of things are copied form offset_ptr

  BufferManagedPtr(pointer ptr = 0) : _page_id(INVALID_PAGE_ID), _offset(0) {
    // TODO: This function totally breaks right now
    if (ptr) {
      const auto [page_id, offset] = BufferManager::get_global_buffer_manager().get_page_id_and_offset_from_ptr(
          reinterpret_cast<const void*>(ptr));
      _page_id = page_id;
      _offset = offset;
      BufferManager::get_global_buffer_manager().pin_page(_page_id);
    }
  }

  BufferManagedPtr(const BufferManagedPtr& ptr) : _page_id(ptr.get_page_id()), _offset(ptr.get_offset()) {
    BufferManager::get_global_buffer_manager().pin_page(_page_id);
  }

  template <class U>
  BufferManagedPtr(const BufferManagedPtr<U>& other) : _page_id(other.get_page_id()), _offset(other.get_offset()) {
    BufferManager::get_global_buffer_manager().pin_page(_page_id);
  }

  template <class T>
  BufferManagedPtr(T* ptr) {
    const auto [page_id, offset] = BufferManager::get_global_buffer_manager().get_page_id_and_offset_from_ptr(ptr);
    _page_id = page_id;
    _offset = offset;
    BufferManager::get_global_buffer_manager().pin_page(_page_id);
  }

  explicit BufferManagedPtr(const PageID page_id, difference_type offset) : _page_id(page_id), _offset(offset) {
    BufferManager::get_global_buffer_manager().pin_page(_page_id);
  }

  ~BufferManagedPtr() {
    if (_page_id == INVALID_PAGE_ID) {
      return;
    }
    BufferManager::get_global_buffer_manager().unpin_page(_page_id);
  }

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

  BufferManagedPtr& operator+=(difference_type offset) noexcept {
    _offset += offset * difference_type(sizeof(PointedType));
    return *this;
  }

  BufferManagedPtr& operator-=(difference_type offset) noexcept {
    _offset -= offset * difference_type(sizeof(PointedType));
    return *this;
  }

  BufferManagedPtr& operator=(const BufferManagedPtr& ptr) {
    _page_id = ptr.get_page_id();
    _offset = ptr.get_offset();
    return *this;
  }

  BufferManagedPtr& operator=(pointer from) {
    // TODO: _offset = reinterpret_cast<difference_type>
    return *this;
  }

  template <class T2>
  BufferManagedPtr& operator=(const BufferManagedPtr<T2>& ptr) {
    _page_id = ptr.get_page_id();
    _offset = ptr.get_offset();
    return *this;
  }

  explicit operator bool() const noexcept {
    return this->_page_id != INVALID_PAGE_ID;  //TODO || this->_offset == 0;
  }

  pointer get() const {
    return static_cast<pointer>(this->get_pointer());
  }

  static BufferManagedPtr pointer_to(reference r) {
    return BufferManagedPtr(std::addressof(r));
  }

  friend bool operator==(const BufferManagedPtr& ptr1, const BufferManagedPtr& ptr2) noexcept {
    return ptr1.get() == ptr2.get();
  }

  friend bool operator==(pointer ptr1, const BufferManagedPtr& ptr2) noexcept {
    return ptr1 == ptr2.get();
  }

  BufferManagedPtr& operator++(void) noexcept {
    _offset += difference_type(sizeof(PointedType));
    return *this;
  }

  BufferManagedPtr operator++(int) noexcept {
    const auto temp = BufferManagedPtr(*this);
    ++*this;
    return temp;
  }

  BufferManagedPtr& operator--(void) noexcept {
    _offset -= difference_type(sizeof(PointedType));
    return *this;
  }

  friend bool operator<(const BufferManagedPtr& ptr1, const BufferManagedPtr& ptr2) noexcept {
    return ptr1.get() < ptr2.get();
  }

  friend bool operator<(pointer& ptr1, const BufferManagedPtr& ptr2) noexcept {
    return ptr1 < ptr2.get();
  }

  friend bool operator<(const BufferManagedPtr& ptr1, pointer ptr2) noexcept {
    return ptr1.get() < ptr2;
  }

  void* get_pointer() const {
    if (_page_id == INVALID_PAGE_ID) {
      return nullptr;
    }
    // TODO: If pinned, this is not needed
    const auto page = BufferManager::get_global_buffer_manager().get_page(_page_id);
    return page->data.data() + _offset;
  }

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
inline std::ptrdiff_t operator-(const BufferManagedPtr<T>& pt, const BufferManagedPtr<T2>& ptr2) {
  return pt.get() - ptr2.get();
}

template <class T1, class T2>
inline bool operator<=(const BufferManagedPtr<T1>& ptr1, const BufferManagedPtr<T2>& ptr2) {
  return ptr1.get() <= ptr2.get();
}

template <class T>
inline void swap(BufferManagedPtr<T>& ptr1, BufferManagedPtr<T>& ptr2) {
  typename BufferManagedPtr<T>::value_type* ptr = ptr1.get();
  ptr1 = ptr2;
  ptr2 = ptr;
}

}  // namespace hyrise

namespace std {

template <class T>
struct hash<hyrise::BufferManagedPtr<T>> {
  size_t operator()(const hyrise::BufferManagedPtr<T>& ptr) const noexcept {
    const auto page_id_hash = std::hash<hyrise::PageID>{}(ptr.get_page_id());
    const auto offset_hash = std::hash<typename hyrise::BufferManagedPtr<T>::difference_type>{}(ptr.get_offset());
    return boost::hash_combine(page_id_hash, offset_hash);
  }
};

}  // namespace std